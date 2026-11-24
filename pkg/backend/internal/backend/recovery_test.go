package internal_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	internal "github.com/canonical/k8s-dqlite/pkg/backend/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/backend/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/limited"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func startServer(t *testing.T, pollInterval time.Duration) (*internal.Backend, *clientv3.Client) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	dir := t.TempDir()

	db, err := sql.Open("sqlite3", (&url.URL{
		Path:     path.Join(dir, "data.db"),
		RawQuery: url.Values{"_journal": {"WAL"}, "_foreign_keys": {"1"}}.Encode(),
	}).String())
	if err != nil {
		t.Fatal(err)
	}
	b, err := sqlite.NewBackend(ctx, &sqlite.BackendConfig{
		Config: limited.Config{
			CompactInterval:   time.Hour,
			PollInterval:      pollInterval,
			WatchQueryTimeout: 20 * time.Second,
		},
		DriverConfig: &sqlite.DriverConfig{DB: database.NewBatched(database.NewPrepared(db))},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// t.TempDir() can exceed the unix socket path limit on macOS.
	sockDir, err := os.MkdirTemp("", "kd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(sockDir) })
	listen := (&url.URL{Scheme: "unix", Path: path.Join(sockDir, "kine.sock")}).String()
	if _, err := endpoint.Listen(ctx, &endpoint.EndpointConfig{
		ListenAddress: listen,
		Server:        limited.New(b, time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{listen}, DialTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return b.(*internal.Backend), client
}

func create(t *testing.T, c *clientv3.Client, key string, opts ...clientv3.OpOption) {
	t.Helper()
	resp, err := c.Txn(context.Background()).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "v", opts...)).
		Commit()
	if err != nil || !resp.Succeeded {
		t.Fatalf("create %s: %v %v", key, resp, err)
	}
}

func expectEvent(t *testing.T, ch clientv3.WatchChan, key string) {
	t.Helper()
	timeout := time.After(10 * time.Second)
	for {
		select {
		case resp, ok := <-ch:
			if !ok || resp.Err() != nil || resp.Canceled {
				t.Fatalf("watch ended waiting for %s: ok=%v err=%v", key, ok, resp.Err())
			}
			for _, ev := range resp.Events {
				if string(ev.Kv.Key) == key {
					return
				}
			}
		case <-timeout:
			t.Fatalf("no event for %s", key)
		}
	}
}

func expectExpired(t *testing.T, c *clientv3.Client, key string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		resp, err := c.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Kvs) == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not expire within %v", key, within)
}

func TestWatchResumesAfterLaggedGroup(t *testing.T) {
	b, c := startServer(t, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := c.Watch(ctx, "/a/", clientv3.WithPrefix())

	create(t, c, "/a/1")
	expectEvent(t, ch, "/a/1")

	b.DropWatcherGroups()
	create(t, c, "/a/2")
	expectEvent(t, ch, "/a/2")
}

func TestTTLAfterLaggedGroup(t *testing.T) {
	b, c := startServer(t, 100*time.Millisecond)
	lease, err := c.Grant(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	b.DropWatcherGroups()
	create(t, c, "/lease/1", clientv3.WithLease(lease.ID))
	expectExpired(t, c, "/lease/1", 10*time.Second)
}

// The TTL watch restarts from a revision that was compacted meanwhile, so
// leases must be listed again.
func TestTTLAfterCompaction(t *testing.T) {
	const pollInterval = 3 * time.Second
	b, c := startServer(t, pollInterval)
	lease, err := c.Grant(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	// The TTL loop now waits pollInterval before it watches again.
	b.DropWatcherGroups()
	start := time.Now()
	create(t, c, "/lease/1", clientv3.WithLease(lease.ID))
	for i := 0; i < internal.SupersededCount+50; i++ {
		create(t, c, fmt.Sprintf("/filler/%d", i))
	}
	if err := b.DoCompact(context.Background()); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > pollInterval {
		t.Skip("setup took longer than the TTL restart backoff")
	}
	expectExpired(t, c, "/lease/1", 15*time.Second)

	// Only the TTL watch's own group is left; failed watches leave none behind.
	if n := b.WatcherGroupCount(); n != 1 {
		t.Fatalf("got %d watcher groups, want 1", n)
	}
}

func TestWatchCompactedRevisionLeavesNoWatcher(t *testing.T) {
	b, c := startServer(t, 100*time.Millisecond)
	create(t, c, "/a/0")
	for i := 0; i < internal.SupersededCount+50; i++ {
		create(t, c, fmt.Sprintf("/filler/%d", i))
	}
	if err := b.DoCompact(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Keeps the client's watch stream, and so its watcher group, open.
	if resp := <-c.Watch(ctx, "/b/", clientv3.WithPrefix(), clientv3.WithCreatedNotify()); !resp.Created {
		t.Fatalf("got %+v, want a created watch", resp)
	}
	before := b.WatcherCount()

	resp := <-c.Watch(ctx, "/a/", clientv3.WithPrefix(), clientv3.WithRev(2))
	if !resp.Canceled {
		t.Fatalf("got %+v, want a cancelled watch", resp)
	}
	t.Logf("watch cancelled: %v", resp.Err())
	if n := b.WatcherCount(); n != before {
		t.Fatalf("got %d watchers, want %d", n, before)
	}
}
