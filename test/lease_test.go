package test

import (
	"context"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLease is unit testing for the lease operation.
func TestLease(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	t.Run("LeaseGrant", func(t *testing.T) {
		g := NewWithT(t)
		var ttl int64 = 300
		resp, err := client.Lease.Grant(ctx, ttl)

		g.Expect(err).To(BeNil())
		g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
		g.Expect(resp.TTL).To(Equal(ttl))
	})

	t.Run("UseLease", func(t *testing.T) {
		var ttl int64 = 1
		t.Run("CreateWithLease", func(t *testing.T) {
			g := NewWithT(t)
			{
				resp, err := client.Lease.Grant(ctx, ttl)

				g.Expect(err).To(BeNil())
				g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
				g.Expect(resp.TTL).To(Equal(ttl))
			}

			{
				resp, err := client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision("/leaseTestKey"), "=", 0)).
					Then(clientv3.OpPut("/leaseTestKey", "testValue", clientv3.WithLease(clientv3.LeaseID(ttl)))).
					Commit()

				g.Expect(err).To(BeNil())
				g.Expect(resp.Succeeded).To(BeTrue())
			}

			{
				resp, err := client.Get(ctx, "/leaseTestKey", clientv3.WithRange(""))
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
				resp, err := client.Get(ctx, "/leaseTestKey", clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				return resp.Kvs
			}, time.Duration(ttl*2)*time.Second, testExpirePollPeriod, ctx).Should(BeEmpty())
		})

	})
}

// BenchmarkLease is a benchmark for the lease operation.
func BenchmarkLease(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)

	g := NewWithT(b)
	for i := 0; i < b.N; i++ {
		var ttl int64 = int64(i + 1)
		resp, err := client.Lease.Grant(ctx, ttl)

		g.Expect(err).To(BeNil())
		g.Expect(resp.ID).To(Equal(clientv3.LeaseID(ttl)))
		g.Expect(resp.TTL).To(Equal(ttl))
	}
}
