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
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _ := newKine(ctx, t)

	createKey(ctx, g, client, "testKey", "testValue")

	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
		Then(clientv3.OpPut("testKey", "testValue2")).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeFalse())
}

// BenchmarkCreate is a benchmark for the Create operation.
func BenchmarkCreate(b *testing.B) {
	b.StopTimer()
	g := NewWithT(b)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _ := newKine(ctx, b)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("value-%d", i)
		createKey(ctx, g, client, key, value)
	}
}

func createKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) int64 {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
	g.Expect(resp.Responses).To(HaveLen(1))
	g.Expect(resp.Responses[0].GetResponsePut()).NotTo(BeNil())
	return resp.Responses[0].GetResponsePut().Header.Revision
}
