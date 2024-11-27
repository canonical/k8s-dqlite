package broadcaster

import (
	"context"
	"sync"
)

type ConnectFunc[T any] func(ctx context.Context) (chan T, error)

type Broadcaster[T any] struct {
	sync.Mutex
	running bool
	subs    map[chan T]struct{}
}

func (b *Broadcaster[T]) Subscribe(ctx context.Context) (<-chan T, error) {
	b.Lock()
	defer b.Unlock()

	sub := make(chan T, 100)
	if b.subs == nil {
		b.subs = map[chan T]struct{}{}
	}
	b.subs[sub] = struct{}{}
	context.AfterFunc(ctx, func() {
		b.Lock()
		defer b.Unlock()
		b.unsub(sub)
	})

	return sub, nil
}

func (b *Broadcaster[T]) unsub(sub chan T) {
	if _, ok := b.subs[sub]; ok {
		close(sub)
		delete(b.subs, sub)
	}
}

func (b *Broadcaster[T]) Start(ctx context.Context, connect ConnectFunc[T]) error {
	b.Lock()
	defer b.Unlock()

	c, err := connect(ctx)
	if err != nil {
		return err
	}

	go b.stream(c)
	b.running = true
	return nil
}

func (b *Broadcaster[T]) stream(ch chan T) {
	for item := range ch {
		b.publish(item)
	}

	b.Lock()
	defer b.Unlock()
	for sub := range b.subs {
		b.unsub(sub)
	}
	b.running = false
}

func (b *Broadcaster[T]) publish(item T) {
	b.Lock()
	defer b.Unlock()

	for sub := range b.subs {
		select {
		case sub <- item:
		default:
			// Slow consumer, drop
			b.unsub(sub)
		}
	}
}
