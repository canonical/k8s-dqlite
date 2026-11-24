package internal

import (
	"context"
	"errors"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/limited"
)

func newTestBackend() *Backend {
	return &Backend{
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*WatcherGroup]*WatcherGroup),
	}
}

func fillUpdates(s *Backend, w *WatcherGroup) {
	for len(w.updates) < cap(w.updates) {
		s.publishEvents(nil)
	}
}

func drain(w *WatcherGroup) int {
	n := 0
	for range w.updates {
		n++
	}
	return n
}

func TestPublishEventsDropsLaggedGroup(t *testing.T) {
	s := newTestBackend()
	g, err := s.WatcherGroup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	w := g.(*WatcherGroup)

	fillUpdates(s, w)
	s.publishEvents(nil)

	if _, ok := s.WatcherGroups[w]; ok {
		t.Fatal("lagged group is still registered")
	}
	if n := drain(w); n != cap(w.updates) {
		t.Fatalf("got %d buffered updates, want %d", n, cap(w.updates))
	}
	if !errors.Is(w.Err(), ErrWatcherLagged) {
		t.Fatalf("Err() = %v, want ErrWatcherLagged", w.Err())
	}
}

// The group's context is cancelled while publishEvents holds s.mu, so stop()
// loses to the AfterFunc. The group must still be unregistered, or the next
// publish sends on its closed channel.
func TestPublishEventsLaggedGroupWithStartedAfterFunc(t *testing.T) {
	s := newTestBackend()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, err := s.WatcherGroup(ctx)
	if err != nil {
		t.Fatal(err)
	}
	w := g.(*WatcherGroup)
	fillUpdates(s, w)

	s.mu.Lock()
	cancel()
	s.publishEventsLocked(nil)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("publish panicked: %v", r)
		}
	}()
	s.publishEventsLocked([]*limited.Event{})
	_, registered := s.WatcherGroups[w]
	s.mu.Unlock()

	if registered {
		t.Fatal("lagged group is still registered")
	}
	drain(w)
	if !errors.Is(w.Err(), ErrWatcherLagged) {
		t.Fatalf("Err() = %v, want ErrWatcherLagged", w.Err())
	}
}
