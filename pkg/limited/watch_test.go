package limited

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type closedWatcherGroup struct {
	updates chan WatcherGroupUpdate
	err     error
}

func (g *closedWatcherGroup) Watch(int64, []byte, []byte, int64) error { return nil }
func (g *closedWatcherGroup) Unwatch(int64)                            {}
func (g *closedWatcherGroup) Updates() <-chan WatcherGroupUpdate       { return g.updates }
func (g *closedWatcherGroup) Err() error                               { return g.err }

func serveClosed(err error) error {
	g := &closedWatcherGroup{updates: make(chan WatcherGroupUpdate), err: err}
	close(g.updates)
	ws := &watchStream{watcherGroup: g}
	return ws.ServeUpdates()
}

func TestServeUpdatesAbnormalCloseIsUnavailable(t *testing.T) {
	err := serveClosed(errors.New("watcher group closed: consumer lagged"))
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("got code %v (%v), want Unavailable", code, err)
	}
}

func TestServeUpdatesCleanClose(t *testing.T) {
	if err := serveClosed(nil); err != nil {
		t.Fatalf("got %v, want nil", err)
	}
}
