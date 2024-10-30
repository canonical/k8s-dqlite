package broadcaster

import (
	"context"
	"sync"
)

type ConnectFunc func(ctx context.Context) (chan interface{}, error)

type Broadcaster struct {
	sync.Mutex
	running bool
	subs    map[chan interface{}]struct{}
}

func (b *Broadcaster) Subscribe(ctx context.Context) (<-chan interface{}, error) {
	b.Lock()
	defer b.Unlock()

	sub := make(chan interface{}, 100)
	if b.subs == nil {
		b.subs = map[chan interface{}]struct{}{}
	}
	b.subs[sub] = struct{}{}
	context.AfterFunc(ctx, func() {
		b.Lock()
		defer b.Unlock()
		b.unsub(sub)
	})

	return sub, nil
}

func (b *Broadcaster) unsub(sub chan interface{}) {
	if _, ok := b.subs[sub]; ok {
		close(sub)
		delete(b.subs, sub)
	}
}

func (b *Broadcaster) Start(ctx context.Context, connect ConnectFunc) error {
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

func (b *Broadcaster) stream(ch chan interface{}) {
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

func (b *Broadcaster) publish(item interface{}) {
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
