package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestUpdate is unit testing for the update operation.
func TestUpdate(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKine(ctx, t, &kineOptions{backendType: backendType})

			// Testing that update can create a new key if ModRevision is 0
			t.Run("UpdateNewKey", func(t *testing.T) {
				g := NewWithT(t)

				createKey(ctx, g, kine.client, "updateNewKey", "testValue")

				resp, err := kine.client.Get(ctx, "updateNewKey", clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("updateNewKey")))
				g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
				g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(resp.Kvs[0].CreateRevision)))
			})

			t.Run("UpdateExisting", func(t *testing.T) {
				g := NewWithT(t)

				lastModRev := createKey(ctx, g, kine.client, "updateExistingKey", "testValue1")
				updateRev(ctx, g, kine.client, "updateExistingKey", lastModRev, "testValue2")

				resp, err := kine.client.Get(ctx, "updateExistingKey", clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("updateExistingKey")))
				g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue2")))
				g.Expect(resp.Kvs[0].ModRevision).To(BeNumerically(">", resp.Kvs[0].CreateRevision))
			})

			// Trying to update an old revision(in compare) should fail
			t.Run("UpdateOldRevisionFails", func(t *testing.T) {
				g := NewWithT(t)

				lastModRev := createKey(ctx, g, kine.client, "updateOldRevKey", "testValue1")
				updateRev(ctx, g, kine.client, "updateOldRevKey", lastModRev, "testValue2")

				resp, err := kine.client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision("updateOldRevKey"), "=", lastModRev)).
					Then(clientv3.OpPut("updateOldRevKey", "testValue2")).
					Else(clientv3.OpGet("updateOldRevKey", clientv3.WithRange(""))).
					Commit()

				g.Expect(err).To(BeNil())
				g.Expect(resp.Succeeded).To(BeFalse())
				g.Expect(resp.Responses).To(HaveLen(1))
				g.Expect(resp.Responses[0].GetResponseRange()).ToNot(BeNil())
			})
		})
	}
}

// BenchmarkUpdate is a benchmark for the Update operation.
func BenchmarkUpdate(b *testing.B) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		b.Run(backendType, func(b *testing.B) {
			b.StopTimer()
			g := NewWithT(b)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKine(ctx, b, &kineOptions{backendType: backendType})

			kine.ResetMetrics()
			b.StartTimer()
			for i, lastModRev := 0, int64(0); i < b.N; i++ {
				value := fmt.Sprintf("value-%d", i)
				lastModRev = updateRev(ctx, g, kine.client, "benchKey", lastModRev, value)
			}
			kine.ReportMetrics(b)
		})
	}
}

func updateKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Get(ctx, key, clientv3.WithRange(""))

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(1))

	updateRev(ctx, g, client, key, resp.Kvs[0].ModRevision, value)
}

func updateRev(ctx context.Context, g Gomega, client *clientv3.Client, key string, revision int64, value string) int64 {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpPut(key, value)).
		Else(clientv3.OpGet(key, clientv3.WithRange(""))).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())

	return resp.Responses[0].GetResponsePut().Header.Revision
}
