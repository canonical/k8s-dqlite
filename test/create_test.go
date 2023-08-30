package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestCreate is unit testing for the create operation.
func TestCreate(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	t.Run("CreateOne", func(t *testing.T) {
		g := NewWithT(t)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
			Then(clientv3.OpPut("testKey", "testValue")).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	})

	t.Run("CreateExistingFails", func(t *testing.T) {
		g := NewWithT(t)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
			Then(clientv3.OpPut("testKey", "testValue2")).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeFalse())
	})
}

// BenchmarkCreate is a benchmark for the Create operation.
func BenchmarkCreate(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)

	g := NewWithT(b)
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, value)).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
	}
}
