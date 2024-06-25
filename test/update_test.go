package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestUpdate is unit testing for the update operation.
func TestUpdate(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	// Testing that update can create a new key if ModRevision is 0
	t.Run("UpdateNewKey", func(t *testing.T) {
		g := NewWithT(t)

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateNewKey"), "=", 0)).
				Then(clientv3.OpPut("updateNewKey", "testValue")).
				Else(clientv3.OpGet("updateNewKey", clientv3.WithRange(""))).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		{
			resp, err := client.Get(ctx, "updateNewKey", clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("updateNewKey")))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue")))
			g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(resp.Kvs[0].CreateRevision)))
		}
	})

	t.Run("UpdateExisting", func(t *testing.T) {
		g := NewWithT(t)

		var lastModRev int64

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateExistingKey"), "=", 0)).
				Then(clientv3.OpPut("updateExistingKey", "testValue1")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
			g.Expect(resp.Responses).To(HaveLen(1))
			g.Expect(resp.Responses[0].GetResponsePut()).NotTo(BeNil())
			lastModRev = resp.Responses[0].GetResponsePut().Header.Revision
		}

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateExistingKey"), "=", lastModRev)).
				Then(clientv3.OpPut("updateExistingKey", "testValue2")).
				Else(clientv3.OpGet("updateExistingKey", clientv3.WithRange(""))).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		{
			resp, err := client.Get(ctx, "updateExistingKey", clientv3.WithRange(""))
			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("updateExistingKey")))
			g.Expect(resp.Kvs[0].Value).To(Equal([]byte("testValue2")))
			g.Expect(resp.Kvs[0].ModRevision).To(BeNumerically(">", resp.Kvs[0].CreateRevision))
		}
	})
	t.Run("UpdateSameKeyLinearity", func(t *testing.T) {
		g := NewWithT(t)

		// Add a large number of entries, two times and
		// it should take approximately same amout of time
		numAddEntries := 1000

		startFirst := time.Now()
		addSameEntries(ctx, g, client, numAddEntries, true)
		durationFirstBatch := time.Since(startFirst)

		startSecond := time.Now()
		addSameEntries(ctx, g, client, numAddEntries, false)
		durationSecondBatch := time.Since(startSecond)

		g.Expect(durationSecondBatch <= durationFirstBatch*2).To(BeTrue())

	})

	// Trying to update an old revision(in compare) should fail
	t.Run("UpdateOldRevisionFails", func(t *testing.T) {
		g := NewWithT(t)

		var lastModRev int64

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateOldRevKey"), "=", 0)).
				Then(clientv3.OpPut("updateOldRevKey", "testValue1")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
			g.Expect(resp.Responses).To(HaveLen(1))
			g.Expect(resp.Responses[0].GetResponsePut()).NotTo(BeNil())
			lastModRev = resp.Responses[0].GetResponsePut().Header.Revision
		}

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateOldRevKey"), "=", lastModRev)).
				Then(clientv3.OpPut("updateOldRevKey", "testValue2")).
				Else(clientv3.OpGet("updateOldRevKey", clientv3.WithRange(""))).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}

		{
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("updateOldRevKey"), "=", lastModRev)).
				Then(clientv3.OpPut("updateOldRevKey", "testValue2")).
				Else(clientv3.OpGet("updateOldRevKey", clientv3.WithRange(""))).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeFalse())
			g.Expect(resp.Responses).To(HaveLen(1))
			g.Expect(resp.Responses[0].GetResponseRange()).ToNot(BeNil())
		}

	})

}

func addSameEntries(ctx context.Context, g Gomega, client *clientv3.Client, numEntries int, create_first bool) {
	for i := 0; i < numEntries; i++ {
		key := "testkey-same"
		value := fmt.Sprintf("value-%d", i)

		if i != 0 || !create_first {
			updateEntry(ctx, g, client, key, value)
		} else {
			addEntry(ctx, g, client, key, value)
		}
	}
}

func updateEntry(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {

	resp, err := client.Get(ctx, key, clientv3.WithRange(""))

	g.Expect(err).To(BeNil())
	g.Expect(resp.Kvs).To(HaveLen(1))

	resp2, err2 := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", resp.Kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, value)).
		Else(clientv3.OpGet(key, clientv3.WithRange(""))).
		Commit()

	g.Expect(err2).To(BeNil())
	g.Expect(resp2.Succeeded).To(BeTrue())

}

func addEntry(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
}

// BenchmarkUpdate is a benchmark for the Update operation.
func BenchmarkUpdate(b *testing.B) {
	ctx := context.Background()
	client, _ := newKine(ctx, b)

	g := NewWithT(b)

	var lastModRev int64 = 0

	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("value-%d", i)
		resp, err := client.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision("benchKey"), "=", lastModRev)).
			Then(clientv3.OpPut("benchKey", value)).
			Else(clientv3.OpGet("benchKey", clientv3.WithRange(""))).
			Commit()

		g.Expect(err).To(BeNil())
		g.Expect(resp.Succeeded).To(BeTrue())
		lastModRev = resp.Responses[0].GetResponsePut().Header.Revision

	}
}
