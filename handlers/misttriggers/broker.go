package misttriggers

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Broker provides an interface for allowing multiple components of the app
// to respond to Mist trigger events without putting all of the application
// logic into the trigger handling code itself.

type Broker interface {
	OnStreamBuffer(func(context.Context, *StreamBuffer) error)

	TriggerStreamBuffer(context.Context, *StreamBuffer) error
}

func NewBroker() Broker {
	return &broker{}
}

type broker struct {
	streamBufferFuncs funcGroup[StreamBuffer]
}

func (b *broker) OnStreamBuffer(cb func(context.Context, *StreamBuffer) error) {
	b.streamBufferFuncs.Register(cb)
}

func (b *broker) TriggerStreamBuffer(ctx context.Context, payload *StreamBuffer) error {
	return b.streamBufferFuncs.Trigger(ctx, payload)
}

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
