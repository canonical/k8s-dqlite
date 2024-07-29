package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestDelete is unit testing for the delete operation.
func TestDelete(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			// Calling the delete method outside a transaction should fail in kine
			t.Run("DeleteNotSupportedFails", func(t *testing.T) {
				g := NewWithT(t)
				resp, err := kine.client.Delete(ctx, "missingKey")

				g.Expect(err).NotTo(BeNil())
				g.Expect(err.Error()).To(ContainSubstring("delete is not supported"))
				g.Expect(resp).To(BeNil())
			})

			// Delete a key that does not exist
			t.Run("DeleteNonExistentKeys", func(t *testing.T) {
				g := NewWithT(t)
				deleteKey(ctx, g, kine.client, "alsoNonExistentKey")
			})

			// Add a key, make sure it exists, then delete it, make sure it got deleted,
			// recreate it, make sure it exists again.
			t.Run("DeleteSuccess", func(t *testing.T) {
				g := NewWithT(t)

				key := "testKeyToDelete"
				value := "testValue"
				createKey(ctx, g, kine.client, key, value)
				assertKey(ctx, g, kine.client, key, value)
				deleteKey(ctx, g, kine.client, key)
				assertMissingKey(ctx, g, kine.client, key)
				createKey(ctx, g, kine.client, key, value)
				assertKey(ctx, g, kine.client, key, value)
			})
		})
	}
}

// BenchmarkDelete is a benchmark for the delete operation.
func BenchmarkDelete(b *testing.B) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		b.Run(backendType, func(b *testing.B) {
			b.StopTimer()
			g := NewWithT(b)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, b, &kineOptions{
				backendType: backendType,
				setup: func(ctx context.Context, tx *sql.Tx) error {
					if err := insertMany(ctx, tx, "key", 100, b.N*2); err != nil {
						return err
					}
					return nil
				},
			})

			kine.ResetMetrics()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("key/%d", i)
				deleteKey(ctx, g, kine.client, key)
			}
			kine.ReportMetrics(b)
		})
	}
}

func assertMissingKey(ctx context.Context, g Gomega, client *clientv3.Client, key string) {
	resp, err := client.Get(ctx, key)

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(0))
}

func deleteKey(ctx context.Context, g Gomega, client *clientv3.Client, key string) {
	// The Get before the Delete is to trick kine to accept the transaction
	resp, err := client.Txn(ctx).
		Then(clientv3.OpGet(key), clientv3.OpDelete(key)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
}

func assertKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Get(ctx, key)

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(1))
	g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
	g.Expect(resp.Kvs[0].Value).To(Equal([]byte(value)))
}
