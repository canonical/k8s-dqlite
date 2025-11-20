package test

import (
	"context"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// testExpirePollPeriod is the polling period for waiting for lease expiration
	testExpirePollPeriod = 100 * time.Millisecond
)

// TestLease is unit testing for the lease operation.
func TestLease(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			t.Run("LeaseGrant", func(t *testing.T) {
				g := NewWithT(t)
				ttl := int64(300)
				resp, err := kine.client.Lease.Grant(ctx, ttl)

				g.Expect(err).To(BeNil())
				g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
				g.Expect(resp.TTL).To(Equal(ttl))
			})

			t.Run("UseLease", func(t *testing.T) {
				ttl := int64(1)
				t.Run("CreateWithLease", func(t *testing.T) {
					g := NewWithT(t)

					{
						resp, err := kine.client.Lease.Grant(ctx, ttl)
						g.Expect(err).To(BeNil())
						g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
						g.Expect(resp.TTL).To(Equal(ttl))
					}

					{
						resp, err := kine.client.Txn(ctx).
							If(clientv3.Compare(clientv3.ModRevision("/leaseTestKey"), "=", 0)).
							Then(clientv3.OpPut("/leaseTestKey", "testValue", clientv3.WithLease(clientv3.LeaseID(ttl)))).
							Commit()
						g.Expect(err).To(BeNil())
						g.Expect(resp.Succeeded).To(BeTrue())
					}

					{
						resp, err := kine.client.Get(ctx, "/leaseTestKey", clientv3.WithRange(""))
						g.Expect(err).To(BeNil())
						g.Expect(resp.Kvs).To(HaveLen(1))
						g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/leaseTestKey")))
						g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
						g.Expect(resp.Kvs[0].Lease).To(Equal(ttl))
					}
				})

				t.Run("KeyShouldExpire", func(t *testing.T) {
					g := NewWithT(t)
					// timeout ttl*2 seconds, poll 100ms
					g.Eventually(func() []*mvccpb.KeyValue {
						resp, err := kine.client.Get(ctx, "/leaseTestKey", clientv3.WithRange(""))
						g.Expect(err).To(BeNil())
						return resp.Kvs
					}, time.Duration(ttl*2500)*time.Millisecond, testExpirePollPeriod, ctx).Should(BeEmpty())
				})
			})
		})
	}
}
