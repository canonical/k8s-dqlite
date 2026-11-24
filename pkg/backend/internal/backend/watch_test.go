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
