package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestList is the unit test for List operation.
func TestList(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	t.Run("ListSuccess", func(t *testing.T) {
		g := NewWithT(t)

		// Create some keys
		keys := []string{"/key/1", "/key/2", "/key/3"}
		for _, key := range keys {
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, "value")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
			g.Expect(resp.Header.Revision).ToNot(BeZero())
		}

		t.Run("ListAll", func(t *testing.T) {
			// Get a list of all the keys
			resp, err := client.Get(ctx, "/key", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(3))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))
			g.Expect(resp.Kvs[2].Key).To(Equal([]byte("/key/3")))
		})

		t.Run("ListPrefix", func(t *testing.T) {
			// Create some keys
			keys := []string{"key/sub/1", "key/sub/2", "key/other/1"}
			for _, key := range keys {
				resp, err := client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
					Then(clientv3.OpPut(key, "value")).
					Commit()

				g.Expect(err).To(BeNil())
				g.Expect(resp.Succeeded).To(BeTrue())
				g.Expect(resp.Header.Revision).ToNot(BeZero())
			}

			// Get a list of all the keys sice they have '/key' prefix
			resp, err := client.Get(ctx, "/key", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(3))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))
			g.Expect(resp.Kvs[2].Key).To(Equal([]byte("/key/3")))

			// Get a list of all the keys sice they have '/key/sub' prefix
			resp, err = client.Get(ctx, "key/sub", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(2))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/sub/1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("key/sub/2")))

			// Get a list of all the keys sice they have '/key/other' prefix
			resp, err = client.Get(ctx, "key/other", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/other/1")))
		})

		t.Run("ListRange", func(t *testing.T) {
			// Get a list of with key/1, as only key/1 falls within the specified range.
			resp, err := client.Get(ctx, "/key/1", clientv3.WithRange(""))

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
		})
	})
}

// BenchmarkList is a benchmark for the Get operation.
func BenchmarkList(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)
	g := NewWithT(b)

	numItems := b.N

	for i := 0; i < numItems; i++ {
		key := fmt.Sprintf("key/%d", i)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "benchValue")).
			Else(clientv3.OpGet(key, clientv3.WithRange(""))).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}

	b.Run("List", func(b *testing.B) {
		g := NewWithT(b)
		for i := 0; i < b.N; i++ {
			resp, err := client.Get(ctx, "key/", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(numItems))
		}
	})
}
