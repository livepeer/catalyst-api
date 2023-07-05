package misttriggers

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
	"golang.org/x/sync/errgroup"
)

// Broker provides an interface for allowing multiple components of the app
// to respond to Mist trigger events without putting all of the application
// logic into the trigger handling code itself.

// There are three different cases to account for on Mist triggers; and
// accordingly the TriggerXXX functions have three different signatures:
// 1. Purely informative, like STREAM_BUFFER. This can't be rejected back
//    to Mist, so no error signature necessary. All of the callbacks can
//    be fired in parallel.
// 2. Allow/deny, like USER_NEW. We either let the new viewer come in
//    or we kick them out, but there's no return value other than that.
//    These handlers can be called in parallel too, but any one of them
//    returning an error will cause an (immediate) trigger rejection.
//    TODO: VID-121
// 3. Triggers with response values, like PUSH_REWRITE. These functions need
//    to return both an error (for rejections) and a string (for responses).
//    They can't be called in parallel; there really should only be one
//    handler for these sorts of triggers.
//    TODO: VID-120

type TriggerBroker interface {
	SetupMistTriggers(clients.MistAPIClient) error

	OnStreamBuffer(func(context.Context, *StreamBufferPayload) error)
	TriggerStreamBuffer(context.Context, *StreamBufferPayload)

	OnPushRewrite(func(context.Context, *PushRewritePayload) (string, error))
	TriggerPushRewrite(context.Context, *PushRewritePayload) (string, error)

	OnLiveTrackList(func(context.Context, *LiveTrackListPayload) error)
	TriggerLiveTrackList(context.Context, *LiveTrackListPayload) error

	// note: an empty string rejects the push. to proceed unchanged, return payload.PushTargetURL
	OnPushOutStart(func(context.Context, *PushOutStartPayload) (string, error))
	TriggerPushOutStart(context.Context, *PushOutStartPayload) (string, error)

	OnPushEnd(func(context.Context, *PushEndPayload) error)
	TriggerPushEnd(context.Context, *PushEndPayload) error

	OnUserNew(func(context.Context, *UserNewPayload) (bool, error))
	TriggerUserNew(context.Context, *UserNewPayload) (string, error)

	OnStreamSource(func(context.Context, *StreamSourcePayload) (string, error))
	TriggerStreamSource(context.Context, *StreamSourcePayload) (string, error)
}

type TriggerPayload interface {
	StreamBufferPayload | PushEndPayload | PushRewritePayload | LiveTrackListPayload | PushOutStartPayload | UserNewPayload | StreamSourcePayload
}

func NewTriggerBroker() TriggerBroker {
	return &triggerBroker{}
}

type triggerBroker struct {
	streamBufferFuncs  funcGroup[StreamBufferPayload]
	pushRewriteFuncs   funcGroup[PushRewritePayload]
	liveTrackListFuncs funcGroup[LiveTrackListPayload]
	pushOutStartFuncs  funcGroup[PushOutStartPayload]
	pushEndFuncs       funcGroup[PushEndPayload]
	userNewFuncs       funcGroup[UserNewPayload]
	streamSourceFuncs  funcGroup[StreamSourcePayload]
}

var triggers = map[string]bool{
	TRIGGER_PUSH_END:        false,
	TRIGGER_PUSH_OUT_START:  true,
	TRIGGER_PUSH_REWRITE:    true,
	TRIGGER_STREAM_BUFFER:   false,
	TRIGGER_LIVE_TRACK_LIST: false,
	TRIGGER_USER_NEW:        true,
	TRIGGER_STREAM_SOURCE:   true,
}

func (b *triggerBroker) SetupMistTriggers(mist clients.MistAPIClient) error {
	for name, sync := range triggers {
		err := mist.AddTrigger([]string{}, name, sync)
		if err != nil {
			return fmt.Errorf("error setting up mist trigger trigger=%s error=%w", name, err)
		}
	}
	return nil
}

func (b *triggerBroker) OnStreamBuffer(cb func(context.Context, *StreamBufferPayload) error) {
	b.streamBufferFuncs.RegisterNoResponse(cb)
}

func (b *triggerBroker) TriggerStreamBuffer(ctx context.Context, payload *StreamBufferPayload) {
	_, err := b.streamBufferFuncs.Trigger(ctx, payload)
	if err != nil {
		glog.Errorf("error handling STREAM_BUFFER trigger: %s", err)
	}
}

func (b *triggerBroker) OnPushRewrite(cb func(context.Context, *PushRewritePayload) (string, error)) {
	b.pushRewriteFuncs.Register(cb)
}

func (b *triggerBroker) TriggerPushRewrite(ctx context.Context, payload *PushRewritePayload) (string, error) {
	return b.pushRewriteFuncs.TriggerWithDefault(ctx, payload, payload.StreamName)
}

func (b *triggerBroker) OnLiveTrackList(cb func(context.Context, *LiveTrackListPayload) error) {
	b.liveTrackListFuncs.RegisterNoResponse(cb)
}

func (b *triggerBroker) TriggerLiveTrackList(ctx context.Context, payload *LiveTrackListPayload) error {
	_, err := b.liveTrackListFuncs.Trigger(ctx, payload)
	return err
}

func (b *triggerBroker) OnPushOutStart(cb func(context.Context, *PushOutStartPayload) (string, error)) {
	b.pushOutStartFuncs.Register(cb)
}

func (b *triggerBroker) TriggerPushOutStart(ctx context.Context, payload *PushOutStartPayload) (string, error) {
	return b.pushOutStartFuncs.Trigger(ctx, payload)
}

func (b *triggerBroker) OnPushEnd(cb func(context.Context, *PushEndPayload) error) {
	b.pushEndFuncs.RegisterNoResponse(cb)
}

func (b *triggerBroker) TriggerPushEnd(ctx context.Context, payload *PushEndPayload) error {
	_, err := b.pushEndFuncs.Trigger(ctx, payload)
	return err
}

func (b *triggerBroker) OnUserNew(cb func(context.Context, *UserNewPayload) (bool, error)) {
	b.userNewFuncs.RegisterBoolean(cb)
}

func (b *triggerBroker) TriggerUserNew(ctx context.Context, payload *UserNewPayload) (string, error) {
	return b.userNewFuncs.Trigger(ctx, payload)
}

func (b *triggerBroker) OnStreamSource(cb func(context.Context, *StreamSourcePayload) (string, error)) {
	b.streamSourceFuncs.Register(cb)
}

func (b *triggerBroker) TriggerStreamSource(ctx context.Context, payload *StreamSourcePayload) (string, error) {
	return b.streamSourceFuncs.Trigger(ctx, payload)
}

// a funcGroup represents a collection of callback functions such that we can register new
// callbacks in a thread-safe manner.
type funcGroup[T TriggerPayload] struct {
	mutex sync.RWMutex
	funcs []func(context.Context, *T) (string, error)
}

// add a function that expects a string response from MIst
func (g *funcGroup[T]) Register(cb func(context.Context, *T) (string, error)) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.funcs = append(g.funcs, cb)
}

// add a function that won't send a string response to Mist
func (g *funcGroup[T]) RegisterNoResponse(cb func(context.Context, *T) error) {
	wrapped := func(ctx context.Context, payload *T) (string, error) {
		err := cb(ctx, payload)
		return "", err
	}
	g.Register(wrapped)
}

// add a function that returns "true" or "false" to Mist
func (g *funcGroup[T]) RegisterBoolean(cb func(context.Context, *T) (bool, error)) {
	wrapped := func(ctx context.Context, payload *T) (string, error) {
		result, err := cb(ctx, payload)
		if result {
			return "true", err
		}
		return "false", err
	}
	g.Register(wrapped)
}

func (g *funcGroup[T]) Trigger(ctx context.Context, payload *T) (string, error) {
	return g.TriggerWithDefault(ctx, payload, "")
}

func (g *funcGroup[T]) TriggerWithDefault(ctx context.Context, payload *T, defaultResponse string) (string, error) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	group, ctx := errgroup.WithContext(ctx)
	ret := defaultResponse
	for i, cb := range g.funcs {
		i := i
		cb := cb
		group.Go(func() error {
			str, err := cb(ctx, payload)
			if err != nil {
				return err
			}
			// Only keep the first return value (see point 3 above)
			if i == 0 {
				ret = str
			}
			return nil
		})
	}
	err := group.Wait()
	if err != nil {
		return "", err
	}
	return ret, err
}
