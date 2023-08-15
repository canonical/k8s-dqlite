package test

import (
	"context"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestGet is unit testing for the Get operation.
func TestGet(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	t.Run("FailNotFound", func(t *testing.T) {
		g := NewWithT(t)
		key := "testKeyFailNotFound"

		// Get non-existent key
		resp, err := client.Get(ctx, key, clientv3.WithRange(""))
		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(BeEmpty())
	})

	t.Run("FailEmptyKey", func(t *testing.T) {
		g := NewWithT(t)

		// Get empty key
		resp, err := client.Get(ctx, "", clientv3.WithRange(""))
		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(HaveLen(0))
	})

	t.Run("FailRange", func(t *testing.T) {
		g := NewWithT(t)
		key := "testKeyFailRange"

		// Get range with a non-existing key
		resp, err := client.Get(ctx, key, clientv3.WithRange("thisIsNotAKey"))
		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(BeEmpty())
	})

	t.Run("Success", func(t *testing.T) {
		g := NewWithT(t)
		key := "testKeySuccess"

		// Create a key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, "testValue")).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// Get key
		{
			resp, err := client.Get(ctx, key, clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
		}
	})

	t.Run("KeyRevision", func(t *testing.T) {
		g := NewWithT(t)
		key := "testKeyRevision"
		var lastModRev int64

		// Create a key with a known value
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, "testValue")).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
			lastModRev = resp.Responses[0].GetResponsePut().Header.Revision
		}

		// Get the key's version
		{
			resp, err := client.Get(ctx, key, clientv3.WithCountOnly())
			g.Expect(err).To(BeNil())
			g.Expect(resp.Count).To(Equal(int64(0)))
		}

		// Update the key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", lastModRev)).
				Then(clientv3.OpPut(key, "testValue2")).
				Else(clientv3.OpGet(key, clientv3.WithRange(""))).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// Get the updated key
		{
			resp, err := client.Get(ctx, key, clientv3.WithCountOnly())
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue2")))
			g.Expect(resp.Kvs[0].ModRevision).To(BeNumerically(">", resp.Kvs[0].CreateRevision))
		}
	})

	t.Run("SuccessWithPrefix", func(t *testing.T) {
		g := NewWithT(t)

		// Create keys with prefix
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("prefix/testKey1"), "=", 0)).
				Then(clientv3.OpPut("prefix/testKey1", "testValue1")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())

			resp, err = client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("prefix/testKey2"), "=", 0)).
				Then(clientv3.OpPut("prefix/testKey2", "testValue2")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// Get keys with prefix
		{
			resp, err := client.Get(ctx, "prefix", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(2))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("prefix/testKey1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("prefix/testKey2")))
		}
	})

	t.Run("FailNotFound", func(t *testing.T) {
		g := NewWithT(t)
		key := "testKeyFailNotFound"

		// Delete key
		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpDelete(key)).
				Else(clientv3.OpGet(key)).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		// Get key
		{
			resp, err := client.Get(ctx, key, clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(BeEmpty())
		}
	})
}

// BenchmarkGet is a benchmark for the Get operation.
func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)
	g := NewWithT(b)

	// create a kv
	{
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
			Then(clientv3.OpPut("testKey", "testValue")).
			Commit()
		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}

	b.Run("LatestRevision", func(b *testing.B) {
		g := NewWithT(b)
		for i := 0; i < b.N; i++ {
			resp, err := client.Get(ctx, "testKey", clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
		}
	})
}
