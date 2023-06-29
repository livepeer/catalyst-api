package misttriggers

import (
	"context"
	"sync"

	"github.com/golang/glog"
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
// 3. Triggers with response values, like PUSH_REWRITE. These functions need
//    to return both an error (for rejections) and a string (for responses).
//    They can't be called in parallel; there really should only be one
//    handler for these sorts of triggers.

type Broker interface {
	OnStreamBuffer(func(context.Context, *StreamBufferPayload) error)

	TriggerStreamBuffer(context.Context, *StreamBufferPayload)
}

func NewBroker() Broker {
	return &broker{}
}

type broker struct {
	streamBufferFuncs funcGroup[StreamBufferPayload]
}

func (b *broker) OnStreamBuffer(cb func(context.Context, *StreamBufferPayload) error) {
	b.streamBufferFuncs.Register(cb)
}

func (b *broker) TriggerStreamBuffer(ctx context.Context, payload *StreamBufferPayload) {
	err := b.streamBufferFuncs.Trigger(ctx, payload)
	if err != nil {
		glog.Errorf("error handling STREAM_BUFFER trigger: %s", err)
	}
}

// a funcGroup represents a collection of callback functions such that we can register new
// callbacks in a thread-safe manner.
type funcGroup[T TriggerPayload] struct {
	mutex sync.RWMutex
	funcs []func(context.Context, *T) error
}

func (g *funcGroup[T]) Register(cb func(context.Context, *T) error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	g.funcs = append(g.funcs, cb)
}

func (g *funcGroup[T]) Trigger(ctx context.Context, payload *T) error {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	group, ctx := errgroup.WithContext(ctx)
	// ...yuck. Is there a better way?
	for _, cb := range g.funcs {
		func(cb func(context.Context, *T) error) {
			group.Go(func() error {
				return cb(ctx, payload)
			})
		}(cb)
	}
	return group.Wait()
}
