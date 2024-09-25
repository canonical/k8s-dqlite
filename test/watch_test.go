package test

import (
	"context"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatch is unit testing for the Watch operation.
func TestWatch(t *testing.T) {
	const (
		// pollTimeout is the timeout for waiting to receive an event.
		pollTimeout = 50 * time.Millisecond

		// idleTimeout is the amount of time to wait to ensure that no events
		// are received when they should not.
		idleTimeout = 100 * time.Millisecond
	)

	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			// start watching for events on key
			const prefix = "test/"
			watchCh := kine.client.Watch(ctx, prefix)

			t.Run("ReceiveNothingUntilActivity", func(t *testing.T) {
				g := NewWithT(t)
				g.Consistently(watchCh, idleTimeout).ShouldNot(Receive())
			})

			t.Run("Create", func(t *testing.T) {
				g := NewWithT(t)

				key := prefix + "createdKey"
				value := "testValue"
				rev := createKey(ctx, g, kine.client, key, value)

				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					CreateEvent(g, key, value, rev),
				))
				g.Consistently(watchCh, idleTimeout).ShouldNot(Receive())
			})

			t.Run("Update", func(t *testing.T) {
				g := NewWithT(t)

				key := prefix + "updatedKey"
				createValue := "testValue1"
				createRev := createKey(ctx, g, kine.client, key, createValue)
				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					CreateEvent(g, key, createValue, createRev),
				))

				updateValue := "testValue2"
				updateRev := updateRev(ctx, g, kine.client, key, createRev, updateValue)
				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					UpdateEvent(g, key, createValue, updateValue, createRev, updateRev),
				))

				g.Consistently(watchCh, idleTimeout).ShouldNot(Receive())
			})

			t.Run("Delete", func(t *testing.T) {
				g := NewWithT(t)

				key := prefix + "deletedKey"
				createValue := "testValue"
				createRev := createKey(ctx, g, kine.client, key, createValue)
				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					CreateEvent(g, key, createValue, createRev),
				))

				deleteRev := deleteKey(ctx, g, kine.client, key, createRev)
				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					DeleteEvent(g, key, createValue, createRev, deleteRev),
				))

				g.Consistently(watchCh, idleTimeout).ShouldNot(Receive())
			})

			t.Run("StartRevision", func(t *testing.T) {
				ctx, cancel := context.WithCancel(ctx)
				defer cancel()
				g := NewWithT(t)

				key := prefix + "revisionKey"
				createValue := "testValue1"
				createRev := createKey(ctx, g, kine.client, key, createValue)

				updateValue := "testValue2"
				updateRev := updateRev(ctx, g, kine.client, key, createRev, updateValue)

				deleteRev := deleteKey(ctx, g, kine.client, key, updateRev)

				watchCh := kine.client.Watch(ctx, key, clientv3.WithRev(createRev))
				g.Eventually(watchCh, pollTimeout).Should(ReceiveEvents(g,
					CreateEvent(g, key, createValue, createRev),
					UpdateEvent(g, key, createValue, updateValue, createRev, updateRev),
					DeleteEvent(g, key, updateValue, updateRev, deleteRev),
				))

				g.Consistently(watchCh, idleTimeout).ShouldNot(Receive())
			})
		})
	}
}

type EventMatcher func(*clientv3.Event) bool

func ReceiveEvents(g Gomega, checks ...EventMatcher) types.GomegaMatcher {
	return Receive(Satisfy(func(watch clientv3.WatchResponse) bool {
		ok := g.Expect(watch.Events).To(HaveLen(len(checks)))
		for i := range checks {
			event := watch.Events[i]
			check := checks[i]
			ok = check(event) && ok
		}
		return ok
	}))
}

func CreateEvent(g Gomega, key, value string, revision int64) EventMatcher {
	return func(event *clientv3.Event) bool {
		ok := g.Expect(event.Type).To(Equal(clientv3.EventTypePut))
		ok = g.Expect(event.Kv.Key).To(Equal([]byte(key))) && ok
		ok = g.Expect(event.Kv.ModRevision).To(Equal(revision)) && ok
		ok = g.Expect(event.Kv.CreateRevision).To(Equal(revision)) && ok
		ok = g.Expect(event.Kv.Value).To(Equal([]byte(value))) && ok
		ok = g.Expect(event.Kv.Version).To(Equal(int64(0))) && ok
		ok = g.Expect(event.PrevKv).To(BeNil()) && ok
		return ok
	}
}

func UpdateEvent(g Gomega, key, prevValue, value string, prevRevision, updateRevision int64) EventMatcher {
	return func(event *clientv3.Event) bool {
		ok := g.Expect(event.Type).To(Equal(clientv3.EventTypePut))
		ok = g.Expect(event.Kv.Key).To(Equal([]byte(key))) && ok
		ok = g.Expect(event.Kv.ModRevision).To(Equal(updateRevision)) && ok
		ok = g.Expect(event.Kv.CreateRevision).To(BeNumerically("<=", prevRevision)) && ok
		ok = g.Expect(event.Kv.Value).To(Equal([]byte(value))) && ok
		ok = g.Expect(event.Kv.Version).To(Equal(int64(0))) && ok

		ok = g.Expect(event.PrevKv).NotTo(BeNil()) && ok
		ok = g.Expect(event.PrevKv.Key).To(Equal([]byte(key))) && ok
		ok = g.Expect(event.PrevKv.ModRevision).To(Equal(prevRevision)) && ok
		ok = g.Expect(event.PrevKv.CreateRevision).To(Equal(event.Kv.CreateRevision)) && ok
		ok = g.Expect(event.PrevKv.Value).To(Equal([]byte(prevValue))) && ok
		ok = g.Expect(event.PrevKv.Version).To(Equal(int64(0))) && ok

		return ok
	}
}

func DeleteEvent(g Gomega, key, prevValue string, prevRevision, deleteRevision int64) EventMatcher {
	return func(event *clientv3.Event) bool {
		ok := g.Expect(event.Type).To(Equal(clientv3.EventTypeDelete))
		ok = g.Expect(event.Kv.Key).To(Equal([]byte(key))) && ok
		ok = g.Expect(event.Kv.ModRevision).To(Equal(deleteRevision)) && ok
		ok = g.Expect(event.Kv.CreateRevision).To(BeNumerically("<=", prevRevision)) && ok
		ok = g.Expect(event.Kv.Value).To(BeNil()) && ok
		ok = g.Expect(event.Kv.Version).To(Equal(int64(0))) && ok

		ok = g.Expect(event.PrevKv).NotTo(BeNil()) && ok
		ok = g.Expect(event.PrevKv.Key).To(Equal([]byte(key))) && ok
		ok = g.Expect(event.PrevKv.ModRevision).To(Equal(prevRevision)) && ok
		ok = g.Expect(event.PrevKv.CreateRevision).To(Equal(event.Kv.CreateRevision)) && ok
		ok = g.Expect(event.PrevKv.Value).To(Equal([]byte(prevValue))) && ok
		ok = g.Expect(event.PrevKv.Version).To(Equal(int64(0))) && ok

		return ok
	}
}
