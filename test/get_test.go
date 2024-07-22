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

// TestGet is unit testing for the Get operation.
func TestGet(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			t.Run("FailNotFound", func(t *testing.T) {
				g := NewWithT(t)
				key := "testKeyFailNotFound"

				// Get non-existent key
				resp, err := kine.client.Get(ctx, key, clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(BeEmpty())
			})

			t.Run("FailEmptyKey", func(t *testing.T) {
				g := NewWithT(t)

				// Get empty key
				resp, err := kine.client.Get(ctx, "", clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(0))
			})

			t.Run("FailRange", func(t *testing.T) {
				g := NewWithT(t)
				key := "testKeyFailRange"

				// Get range with a non-existing key
				resp, err := kine.client.Get(ctx, key, clientv3.WithRange("thisIsNotAKey"))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(BeEmpty())
			})

			t.Run("Success", func(t *testing.T) {
				g := NewWithT(t)
				key := "testKeySuccess"

				createKey(ctx, g, kine.client, key, "testValue")

				resp, err := kine.client.Get(ctx, key, clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
				g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
			})

			t.Run("KeyRevision", func(t *testing.T) {
				g := NewWithT(t)
				key := "testKeyRevision"
				lastModRev := createKey(ctx, g, kine.client, key, "testValue")

				// Get the key's version
				resp, err := kine.client.Get(ctx, key, clientv3.WithCountOnly())
				g.Expect(err).To(BeNil())
				g.Expect(resp.Count).To(Equal(int64(0)))

				updateRev(ctx, g, kine.client, key, lastModRev, "testValue2")

				// Get the updated key
				resp, err = kine.client.Get(ctx, key, clientv3.WithCountOnly())
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue2")))
				g.Expect(resp.Kvs[0].ModRevision).To(BeNumerically(">", resp.Kvs[0].CreateRevision))
			})

			t.Run("SuccessWithPrefix", func(t *testing.T) {
				g := NewWithT(t)

				// Create keys with prefix
				createKey(ctx, g, kine.client, "prefix/testKey1", "testValue1")
				createKey(ctx, g, kine.client, "prefix/testKey2", "testValue2")

				resp, err := kine.client.Get(ctx, "prefix", clientv3.WithPrefix())

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("prefix/testKey1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("prefix/testKey2")))
			})

			t.Run("FailNotFound", func(t *testing.T) {
				g := NewWithT(t)
				key := "testKeyFailNotFound"

				// Delete key
				deleteKey(ctx, g, kine.client, key)

				// Get key
				resp, err := kine.client.Get(ctx, key, clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(BeEmpty())
			})
		})
	}
}

// BenchmarkGet is a benchmark for the Get operation.
func BenchmarkGet(b *testing.B) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		b.Run(backendType, func(b *testing.B) {
			b.StopTimer()
			g := NewWithT(b)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, b, &kineOptions{
				backendType: backendType,
				setup: func(ctx context.Context, tx *sql.Tx) error {
					if err := insertMany(ctx, tx, "testKey", 100, b.N*2); err != nil {
						return err
					}
					if err := updateMany(ctx, tx, "testKey", 100, b.N); err != nil {
						return err
					}
					return nil
				},
			})

			kine.ResetMetrics()
			b.StartTimer()
			for i := 0; i < b.N; i++ {
				resp, err := kine.client.Get(ctx, fmt.Sprintf("testKey/%d", i+1), clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
			}
			kine.ReportMetrics(b)
		})
	}
}
