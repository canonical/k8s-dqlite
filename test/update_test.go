package test

import (
	"context"
	"fmt"
	"sync"
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

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			t.Run("UpdateExisting", func(t *testing.T) {
				g := NewWithT(t)

				lastModRev := createKey(ctx, g, kine.client, "updateExistingKey", "testValue1")
				updateRev(ctx, g, kine.client, "updateExistingKey", lastModRev, "testValue2")

				resp, err := kine.client.Get(ctx, "updateExistingKey", clientv3.WithRange(""))
				g.Expect(err).ToNot(HaveOccurred())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("updateExistingKey")))
				g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue2")))
				g.Expect(resp.Kvs[0].ModRevision).To(BeNumerically(">", resp.Kvs[0].CreateRevision))
			})

			t.Run("CreateExistingFails", func(t *testing.T) {
				g := NewWithT(t)

				createKey(ctx, g, kine.client, "createExistingKey", "testValue1")

				resp, err := kine.client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision("createExistingKey"), "=", 0)).
					Then(clientv3.OpPut("createExistingKey", "testValue1")).
					Else(clientv3.OpGet("createExistingKey", clientv3.WithRange(""))).
					Commit()

				g.Expect(err).ToNot(HaveOccurred())
				g.Expect(resp.Succeeded).To(BeFalse())
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

				g.Expect(err).ToNot(HaveOccurred())
				g.Expect(resp.Succeeded).To(BeFalse())
				g.Expect(resp.Responses).To(HaveLen(1))
				g.Expect(resp.Responses[0].GetResponseRange()).ToNot(BeNil())
			})

			t.Run("UpdateNotExistingFails", func(t *testing.T) {
				g := NewWithT(t)

				resp, err := kine.client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision("updateNotExistingKey"), "=", 1)).
					Then(clientv3.OpPut("updateNotExistingKey", "testValue3")).
					Else(clientv3.OpGet("updateNotExistingKey", clientv3.WithRange(""))).
					Commit()

				g.Expect(err).ToNot(HaveOccurred())
				g.Expect(resp.Succeeded).To(BeFalse())
				g.Expect(resp.Responses).To(HaveLen(1))
				g.Expect(resp.Responses[0].GetResponseRange()).ToNot(BeNil())
			})

			t.Run("UpdatedDeletedKeyFails", func(t *testing.T) {
				g := NewWithT(t)

				lastModRev := createKey(ctx, g, kine.client, "updateDeletedKey", "testValue4")
				lastModRev = deleteKey(ctx, g, kine.client, "updateDeletedKey", lastModRev)

				resp, err := kine.client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision("updateDeletedKey"), "=", lastModRev)).
					Then(clientv3.OpPut("updateDeletedKey", "testValue4")).
					Else(clientv3.OpGet("updateDeletedKey", clientv3.WithRange(""))).
					Commit()

				g.Expect(err).ToNot(HaveOccurred())
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
		for _, workers := range []int{1, 4, 16, 64, 128} {
			b.Run(fmt.Sprintf("%s/%d-workers", backendType, workers), func(b *testing.B) {
				b.StopTimer()
				g := NewWithT(b)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				kine := newKineServer(ctx, b, &kineOptions{backendType: backendType})
				wg := &sync.WaitGroup{}
				run := func(start int) {
					defer wg.Done()
					benchKey := fmt.Sprintf("benchKey-%d", start)
					for i, lastModRev := start, int64(0); i < b.N; i += workers {
						value := fmt.Sprintf("value-%d", i)
						lastModRev = updateRev(ctx, g, kine.client, benchKey, lastModRev, value)
					}
				}

				kine.ResetMetrics()
				b.StartTimer()
				wg.Add(workers)
				for worker := 0; worker < workers; worker++ {
					go run(worker)
				}
				wg.Wait()
				kine.ReportMetrics(b)
			})
		}
	}
}

func updateRev(ctx context.Context, g Gomega, client *clientv3.Client, key string, revision int64, value string) int64 {
	txn := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpPut(key, value))
	if revision != 0 {
		txn = txn.Else(clientv3.OpGet(key, clientv3.WithRange("")))
	}
	resp, err := txn.Commit()

	g.Expect(err).ToNot(HaveOccurred())
	g.Expect(resp.Succeeded).To(BeTrue())

	return resp.Responses[0].GetResponsePut().Header.Revision
}
