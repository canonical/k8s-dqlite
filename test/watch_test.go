package test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestWatch is unit testing for the Watch operation.
func TestWatch(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	var (
		revAfterCreate int64
		revAfterUpdate int64
		revAfterDelete int64

		key          = "testKey"
		value        = "testValue"
		updatedValue = "testUpdatedValue"
	)

	// start watching for events on key
	watchCh := client.Watch(ctx, key)

	t.Run("ReceiveNothingUntilActivity", func(t *testing.T) {
		g := NewWithT(t)
		g.Consistently(watchCh, testWatchEventIdleTimeout).ShouldNot(Receive())
	})

	t.Run("Create", func(t *testing.T) {
		g := NewWithT(t)

		// create a key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, value)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// receive event
		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)
			g.Eventually(watchCh, testWatchEventPollTimeout).Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				g.Expect(v.Events).To(HaveLen(1))
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypePut))
				g.Expect(v.Events[0].PrevKv).To(BeNil())
				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(value)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))

				revAfterCreate = v.Events[0].Kv.ModRevision

				return true
			})))
		})

		t.Run("ReceiveNothingUntilNewActivity", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, testWatchEventIdleTimeout).ShouldNot(Receive())
		})
	})

	t.Run("Update", func(t *testing.T) {
		g := NewWithT(t)

		// update key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", revAfterCreate)).
				Then(clientv3.OpPut(key, string(updatedValue))).
				Else(clientv3.OpGet(key)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)

			// receive event
			g.Eventually(watchCh, testWatchEventPollTimeout).Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				g.Expect(v.Events).To(HaveLen(1))
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypePut))
				g.Expect(v.Events[0].PrevKv).NotTo(BeNil())
				g.Expect(v.Events[0].PrevKv.Value).To(Equal([]byte(value)))
				g.Expect(v.Events[0].PrevKv.ModRevision).To(Equal(revAfterCreate))

				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))
				g.Expect(v.Events[0].Kv.ModRevision).To(BeNumerically(">", revAfterCreate))

				revAfterUpdate = v.Events[0].Kv.ModRevision

				return true
			})))
		})

		t.Run("ReceiveNothingUntilNewActivity", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, testWatchEventIdleTimeout).ShouldNot(Receive())
		})
	})

	t.Run("Delete", func(t *testing.T) {
		g := NewWithT(t)

		// delete key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", revAfterUpdate)).
				Then(clientv3.OpDelete(key)).
				Else(clientv3.OpGet(key)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)

			// receive event
			g.Eventually(watchCh, testWatchEventPollTimeout).Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				g.Expect(v.Events).To(HaveLen(1))
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypeDelete))
				g.Expect(v.Events[0].PrevKv).NotTo(BeNil())
				g.Expect(v.Events[0].PrevKv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[0].PrevKv.ModRevision).To(Equal(revAfterUpdate))

				g.Expect(v.Events[0].Kv).NotTo(BeNil())
				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))
				g.Expect(v.Events[0].Kv.ModRevision).To(BeNumerically(">", revAfterUpdate))

				revAfterDelete = v.Events[0].Kv.ModRevision

				return true
			})))
		})

		t.Run("ReceiveNothingUntilNewActivity", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, testWatchEventIdleTimeout).ShouldNot(Receive())
		})
	})

	t.Run("StartRevision", func(t *testing.T) {
		watchAfterDeleteCh := client.Watch(ctx, key, clientv3.WithRev(revAfterUpdate))

		t.Run("Receive", func(t *testing.T) {
			g := NewWithT(t)

			g.Eventually(watchAfterDeleteCh, testWatchEventPollTimeout).Should(Receive(Satisfy(func(v clientv3.WatchResponse) bool {
				// receive 2 events
				g.Expect(v.Events).To(HaveLen(2))

				// receive update event
				g.Expect(v.Events[0].Type).To(Equal(clientv3.EventTypePut))
				g.Expect(v.Events[0].PrevKv).NotTo(BeNil())
				g.Expect(v.Events[0].PrevKv.Value).To(Equal([]byte(value)))
				g.Expect(v.Events[0].PrevKv.ModRevision).To(Equal(revAfterCreate))

				g.Expect(v.Events[0].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[0].Kv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[0].Kv.Version).To(Equal(int64(0)))
				g.Expect(v.Events[0].Kv.ModRevision).To(Equal(revAfterUpdate))

				// receive delete event
				g.Expect(v.Events[1].Type).To(Equal(clientv3.EventTypeDelete))
				g.Expect(v.Events[1].PrevKv).NotTo(BeNil())
				g.Expect(v.Events[1].PrevKv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[1].PrevKv.ModRevision).To(Equal(revAfterUpdate))

				g.Expect(v.Events[1].Kv).NotTo(BeNil())
				g.Expect(v.Events[1].Kv.Key).To(Equal([]byte(key)))
				g.Expect(v.Events[1].Kv.Value).To(Equal([]byte(updatedValue)))
				g.Expect(v.Events[1].Kv.Version).To(Equal(int64(0)))
				g.Expect(v.Events[1].Kv.ModRevision).To(Equal(revAfterDelete))

				return true
			})))
		})

		t.Run("OtherWatcherIdle", func(t *testing.T) {
			g := NewWithT(t)
			g.Consistently(watchCh, testWatchEventIdleTimeout).ShouldNot(Receive())
		})
	})
}
