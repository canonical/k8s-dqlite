package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestDelete is unit testing for the delete operation.
func TestDelete(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	// Calling the delete method outside a transaction should fail in kine
	t.Run("DeleteNotSupportedFails", func(t *testing.T) {
		g := NewWithT(t)
		resp, err := client.Delete(ctx, "missingKey")

		g.Expect(err).NotTo(BeNil())
		g.Expect(err.Error()).To(ContainSubstring("delete is not supported"))
		g.Expect(resp).To(BeNil())
	})

	// Delete a key that does not exist
	t.Run("DeleteNonExistentKeys", func(t *testing.T) {
		g := NewWithT(t)
		deleteKey(ctx, g, client, "alsoNonExistentKey")
	})

	// Add a key, make sure it exists, then delete it, make sure it got deleted,
	// recreate it, make sure it exists again.
	t.Run("DeleteSuccess", func(t *testing.T) {
		g := NewWithT(t)

		key := "testKeyToDelete"
		value := "testValue"
		createKey(ctx, g, client, key, value)
		assertKey(ctx, g, client, key, value)
		deleteKey(ctx, g, client, key)
		assertMissingKey(ctx, g, client, key)
		createKey(ctx, g, client, key, value)
		assertKey(ctx, g, client, key, value)
	})
}

// BenchmarkDelete is a benchmark for the delete operation.
func BenchmarkDelete(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)

	g := NewWithT(b)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		createKey(ctx, g, client, key, value)
	}

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		deleteKey(ctx, g, client, key)
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

func createKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
}
