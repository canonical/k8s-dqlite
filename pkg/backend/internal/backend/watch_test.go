package internal_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync/atomic"
	"testing"
	"time"

	internal "github.com/canonical/k8s-dqlite/pkg/backend/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/backend/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/limited"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// faultyDriver fails AfterPrefix on demand.
type faultyDriver struct {
	*sqlite.Driver
	fail atomic.Bool
}

func (d *faultyDriver) AfterPrefix(ctx context.Context, key, rangeEnd []byte, from, to int64) (*sql.Rows, error) {
	if d.fail.Load() {
		return nil, errors.New("injected error")
	}
	return d.Driver.AfterPrefix(ctx, key, rangeEnd, from, to)
}

// newWatchTestServer starts a sqlite backend that polls only when notified of
// local writes, and a client connected to it.
func newWatchTestServer(t *testing.T) (*clientv3.Client, *faultyDriver) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// t.TempDir() can exceed the unix socket path limit on macOS.
	dir, err := os.MkdirTemp("", "kd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	db, err := sql.Open("sqlite3", path.Join(dir, "data.db")+"?_journal=WAL")
	if err != nil {
		t.Fatal(err)
	}
	driver, err := sqlite.NewDriver(ctx, &sqlite.DriverConfig{DB: database.NewBatched(database.NewPrepared(db))})
	if err != nil {
		t.Fatal(err)
	}
	fd := &faultyDriver{Driver: driver}
	b := &internal.Backend{
		Config: limited.Config{
			CompactInterval:   time.Hour,
			PollInterval:      time.Hour,
			WatchQueryTimeout: 20 * time.Second,
		},
		Driver:        fd,
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*internal.WatcherGroup]*internal.WatcherGroup),
	}
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })

	listen := (&url.URL{Scheme: "unix", Path: path.Join(dir, "kine.sock")}).String()
	if _, err := endpoint.Listen(ctx, &endpoint.EndpointConfig{
		ListenAddress: listen,
		Server:        limited.New(b, time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{listen}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return client, fd
}

func putKey(t *testing.T, c *clientv3.Client, key string) int64 {
	t.Helper()
	ctx := context.Background()
	resp, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v")).
		Commit()
	if err != nil || !resp.Succeeded {
		t.Fatalf("put %s: %v %v", key, resp, err)
	}
	return resp.Header.Revision
}

// collect returns what the watch delivers within d, failing on a closed channel.
func collect(t *testing.T, ch clientv3.WatchChan, d time.Duration) []clientv3.WatchResponse {
	t.Helper()
	var got []clientv3.WatchResponse
	timeout := time.After(d)
	for {
		select {
		case resp, ok := <-ch:
			if !ok {
				t.Fatal("watch channel closed")
			}
			got = append(got, resp)
		case <-timeout:
			return got
		}
	}
}

// The poll can be behind the database: writes through another node don't
// notify it. A watch starting after a list must still get only newer events.
func TestWatchAheadOfPollSkipsOlderEvents(t *testing.T) {
	c, fd := newWatchTestServer(t)
	ctx := context.Background()
	putKey(t, c, "/a/0")
	for _, key := range []string{"/a/1", "/a/2"} {
		if _, _, err := fd.Driver.Create(ctx, []byte(key), []byte("v"), 0); err != nil {
			t.Fatal(err)
		}
	}
	list, err := c.Get(ctx, "/a/", clientv3.WithPrefix())
	if err != nil {
		t.Fatal(err)
	}
	listRev := list.Header.Revision

	ch := c.Watch(ctx, "/a/", clientv3.WithPrefix(), clientv3.WithRev(listRev+1))
	time.Sleep(200 * time.Millisecond)
	putKey(t, c, "/a/3")

	var keys []string
	for _, resp := range collect(t, ch, time.Second) {
		for _, ev := range resp.Events {
			keys = append(keys, string(ev.Kv.Key))
			if ev.Kv.ModRevision <= listRev {
				t.Errorf("got %s at revision %d, watch started at %d", ev.Kv.Key, ev.Kv.ModRevision, listRev+1)
			}
		}
	}
	if len(keys) != 1 || keys[0] != "/a/3" {
		t.Errorf("got events %v, want [/a/3]", keys)
	}
}

// A progress notification must not get ahead of a new watcher's catch-up events.
func TestProgressAfterCatchUpEvents(t *testing.T) {
	c, _ := newWatchTestServer(t)
	ctx := context.Background()
	putKey(t, c, "/b/1")
	putKey(t, c, "/b/2")
	// Opens the stream and moves its revision past /b/.
	c.Watch(ctx, "/other")
	time.Sleep(200 * time.Millisecond)
	putKey(t, c, "/c")
	time.Sleep(200 * time.Millisecond)

	ch := c.Watch(ctx, "/b/", clientv3.WithPrefix(), clientv3.WithRev(1))
	time.Sleep(200 * time.Millisecond)
	if err := c.RequestProgress(ctx); err != nil {
		t.Fatal(err)
	}

	var events int
	var progress bool
	for _, resp := range collect(t, ch, time.Second) {
		if resp.IsProgressNotify() {
			progress = true
			continue
		}
		for _, ev := range resp.Events {
			events++
			if progress {
				t.Errorf("got %s at revision %d after a progress notification", ev.Kv.Key, ev.Kv.ModRevision)
			}
		}
	}
	if events != 2 || !progress {
		t.Errorf("got %d events and progress=%v, want 2 events and a progress notification", events, progress)
	}
}

// A watch that fails to be created must not block later watches on its stream.
func TestFailedWatchDoesNotBlockStream(t *testing.T) {
	c, fd := newWatchTestServer(t)
	ctx := context.Background()
	putKey(t, c, "/x")

	fd.fail.Store(true)
	failed := watchAsync(t, c, ctx, "/x", clientv3.WithRev(1))
	select {
	case resp, ok := <-failed:
		if ok && !resp.Canceled {
			t.Fatalf("got %+v, want a cancelled watch", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failed watch got no response")
	}
	fd.fail.Store(false)

	ch := watchAsync(t, c, ctx, "/y", clientv3.WithCreatedNotify())
	select {
	case resp := <-ch:
		if !resp.Created {
			t.Fatalf("got %+v, want a created watch", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("later watch on the same stream was never created")
	}
	putKey(t, c, "/y")
	var keys []string
	for _, resp := range collect(t, ch, time.Second) {
		for _, ev := range resp.Events {
			keys = append(keys, string(ev.Kv.Key))
		}
	}
	if fmt.Sprint(keys) != "[/y]" {
		t.Errorf("got events %v, want [/y]", keys)
	}
}

// watchAsync returns the watch channel, or fails if clientv3 blocks creating it.
func watchAsync(t *testing.T, c *clientv3.Client, ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan {
	t.Helper()
	done := make(chan clientv3.WatchChan, 1)
	go func() { done <- c.Watch(ctx, key, opts...) }()
	select {
	case ch := <-done:
		return ch
	case <-time.After(5 * time.Second):
		t.Fatalf("Watch(%s) blocked", key)
		return nil
	}
}

// The TTL watch gets its catch-up events from Watch, not from an update.
func TestLeaseExpires(t *testing.T) {
	c, _ := newWatchTestServer(t)
	ctx := context.Background()
	lease, err := c.Grant(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/lease"), "=", 0)).
		Then(clientv3.OpPut("/lease", "v", clientv3.WithLease(lease.ID))).
		Commit(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := c.Get(ctx, "/lease")
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("/lease did not expire")
}
