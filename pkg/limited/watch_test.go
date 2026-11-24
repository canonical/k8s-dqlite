package limited

import (
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type recordingServer struct {
	etcdserverpb.Watch_WatchServer
	sent []*etcdserverpb.WatchResponse
}

func (s *recordingServer) Send(resp *etcdserverpb.WatchResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

type catchUpGroup struct {
	revision int64
	events   []*Event
}

func (g *catchUpGroup) Watch(int64, []byte, []byte, int64) (int64, []*Event, error) {
	return g.revision, g.events, nil
}
func (g *catchUpGroup) Unwatch(int64)                      {}
func (g *catchUpGroup) Updates() <-chan WatcherGroupUpdate { return nil }

func isProgress(resp *etcdserverpb.WatchResponse) bool {
	return resp.WatchId == clientv3.InvalidWatchID && !resp.Created && !resp.Canceled && len(resp.Events) == 0
}

// Catch-up events at revision 10 are sent while updates up to 5 were sent so
// far: a progress request waits until the stream has sent revision 10.
func TestProgressDeferredBelowCatchUpRevision(t *testing.T) {
	srv := &recordingServer{}
	ws := &watchStream{
		watcherGroup: &catchUpGroup{
			revision: 10,
			events:   []*Event{{Kv: &KeyValue{Key: []byte("/a"), ModRevision: 8}}},
		},
		server:             srv,
		sendProgressReport: map[int64]bool{},
	}
	if err := ws.sendUpdates(5, nil); err != nil {
		t.Fatal(err)
	}
	if err := ws.Create(1, []byte("/a"), []byte("/b"), 1); err != nil {
		t.Fatal(err)
	}
	sent := len(srv.sent)

	if err := ws.RequestProgress(); err != nil {
		t.Fatal(err)
	}
	if err := ws.sendProgress(); err != nil {
		t.Fatal(err)
	}
	if len(srv.sent) != sent {
		t.Fatalf("progress sent at revision %d, below the catch-up revision 10", ws.revision)
	}

	other := []WatcherUpdate{{WatcherId: 2, Events: []*mvccpb.Event{{Kv: &KeyValue{Key: []byte("/z"), ModRevision: 7}}}}}
	if err := ws.sendUpdates(10, other); err != nil {
		t.Fatal(err)
	}
	last := srv.sent[len(srv.sent)-1]
	if !isProgress(last) || last.Header.Revision != 10 {
		t.Fatalf("got %+v, want the deferred progress at revision 10", last)
	}
}
