package test

import (
	"context"
	"testing"
	"time"

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
	const leaseKey = "/leaseTestKey"
	const leaseValue = "testValue"
	const ttlSeconds = 1

	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			server := newK8sDqliteServer(ctx, t, &k8sDqliteConfig{backendType: backendType})

			g := NewWithT(t)
			lease := grantLease(ctx, g, server.client, ttlSeconds)

			createKey(ctx, g, server.client, leaseKey, leaseValue, clientv3.WithLease(lease))

			resp, err := server.client.Get(ctx, leaseKey, clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte(leaseKey)))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte(leaseValue)))
			g.Expect(resp.Kvs[0].Lease).To(Equal(int64(lease)))

			g.Eventually(func() []*mvccpb.KeyValue {
				resp, err := server.client.Get(ctx, leaseKey, clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				return resp.Kvs
			}, time.Duration(ttlSeconds*2)*time.Second, testExpirePollPeriod, ctx).Should(BeEmpty())
		})
	}
}

func grantLease(ctx context.Context, g Gomega, client *clientv3.Client, ttl int64) clientv3.LeaseID {
	resp, err := client.Lease.Grant(ctx, ttl)

	g.Expect(err).To(BeNil())
	g.Expect(resp.TTL).To(Equal(ttl))

	return resp.ID
}
