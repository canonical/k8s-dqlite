package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestDelete is unit testing for the delete operation.
func TestDelete(t *testing.T) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineConfig{backendType: backendType})

			// Calling the delete method outside a transaction should fail in kine
			t.Run("NotSupportedFails", func(t *testing.T) {
				g := NewWithT(t)
				resp, err := kine.client.Delete(ctx, "missingKey")

				g.Expect(err).NotTo(BeNil())
				g.Expect(err.Error()).To(ContainSubstring("delete is not supported"))
				g.Expect(resp).To(BeNil())
			})

			// Delete a key that does not exist
			t.Run("NonExistentKeys", func(t *testing.T) {
				g := NewWithT(t)
				key := "missing key"
				rev := 0
				resp, err := kine.client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", rev)).
					Then(clientv3.OpDelete(key)).
					Else(clientv3.OpGet(key)).
					Commit()

				g.Expect(err).To(BeNil())
				g.Expect(resp.Succeeded).To(BeFalse())
			})

			// Add a key, make sure it exists, then delete it, make sure it got deleted,
			// recreate it, make sure it exists again.
			t.Run("Success", func(t *testing.T) {
				g := NewWithT(t)

				key := "testKeyToDelete"
				value := "testValue"
				rev := createKey(ctx, g, kine.client, key, value)
				assertKey(ctx, g, kine.client, key, value)
				deleteKey(ctx, g, kine.client, key, rev)
				assertMissingKey(ctx, g, kine.client, key)
				createKey(ctx, g, kine.client, key, value)
				assertKey(ctx, g, kine.client, key, value)
			})
		})
	}
}

// BenchmarkDelete is a benchmark for the delete operation.
func BenchmarkDelete(b *testing.B) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		for _, workers := range []int{1, 4, 16, 64, 128} {
			b.Run(fmt.Sprintf("%s/%d-workers", backendType, workers), func(b *testing.B) {
				b.StopTimer()
				g := NewWithT(b)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var initialRev int64
				kine := newKineServer(ctx, b, &kineConfig{
					backendType: backendType,
					setup: func(ctx context.Context, tx *sql.Tx) error {
						if lastRev, err := insertMany(ctx, tx, "key", 100, b.N*2); err != nil {
							return err
						} else {
							initialRev = lastRev - int64(b.N)*2
							return nil
						}
					},
				})
				wg := &sync.WaitGroup{}
				run := func(start int) {
					defer wg.Done()
					for i := start; i < b.N; i += workers {
						keyId := int64(i + 1)
						key := fmt.Sprintf("key/%d", keyId)
						deleteKey(ctx, g, kine.client, key, initialRev+keyId)
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

func assertMissingKey(ctx context.Context, g Gomega, client *clientv3.Client, key string) {
	resp, err := client.Get(ctx, key)

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(0))
}

func deleteKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, revision int64) int64 {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", revision)).
		Then(clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
	return resp.Header.Revision
}

func assertKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Get(ctx, key)

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(1))
	g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
	g.Expect(resp.Kvs[0].Value).To(Equal([]byte(value)))
}
