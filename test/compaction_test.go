package test

import (
	"context"
	"fmt"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCompaction(t *testing.T) {
	ctx := context.Background()
	client, backend := newKine(ctx, t)

	t.Run("SmallDatabaseDeleteEntry", func(t *testing.T) {
		g := NewWithT(t)

		// Add a few entries
		numEntries := 2
		addEntries(ctx, g, client, numEntries)

		// Delete an entry
		keyNo := 0
		key := fmt.Sprintf("testkey-%d", keyNo)
		deleteEntry(ctx, g, client, key)

		initialSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		err = backend.DoCompact()
		g.Expect(err).To(BeNil())

		finalSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		// Expecting no compaction
		g.Expect(finalSize).To(BeNumerically("==", initialSize))

		// Delete the remaining entry before the next test
		deleteEntries(ctx, g, client, 1, 2)
	})

	t.Run("LargeDatabaseDeleteFivePercent", func(t *testing.T) {
		g := NewWithT(t)

		// Add a large number of entries
		numAddEntries := 100_000
		addEntries(ctx, g, client, numAddEntries)

		// Delete 5% of the entries
		numDelEntries := 5000
		start := 0
		deleteEntries(ctx, g, client, start, start+numDelEntries)

		initialSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		err = backend.DoCompact()
		g.Expect(err).To(BeNil())

		finalSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		// Expecting compaction
		g.Expect(finalSize).To(BeNumerically("<", initialSize))
	})
}

func addEntries(ctx context.Context, g Gomega, client *clientv3.Client, numEntries int) {
	for i := 0; i < numEntries; i++ {
		key := fmt.Sprintf("testkey-%d", i)
		value := fmt.Sprintf("value-%d", i)
		addEntry(ctx, g, client, key, value)
	}
}

func addEntry(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string) {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
}

func deleteEntries(ctx context.Context, g Gomega, client *clientv3.Client, start int, end int) {
	for i := start; i < end; i++ {
		key := fmt.Sprintf("testkey-%d", i)
		deleteEntry(ctx, g, client, key)
	}
}

func deleteEntry(ctx context.Context, g Gomega, client *clientv3.Client, key string) {
	// Get the entry before calling Delete, to trick Kine to accept the transaction
	resp, err := client.Txn(ctx).
		Then(clientv3.OpGet(key), clientv3.OpDelete(key)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
}

func BenchmarkCompaction(b *testing.B) {
	ctx := context.Background()
	client, backend := newKine(ctx, b)
	g := NewWithT(b)

	numAddEntries := 100_000
	numDelEntries := 5000

	for i := 0; i < b.N; i++ {
		// Add a large number of entries
		addEntries(ctx, g, client, numAddEntries)

		// Delete 5% of the entries
		deleteEntries(ctx, g, client, 0, numDelEntries)

		initialSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		err = backend.DoCompact()
		g.Expect(err).To(BeNil())

		finalSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		// Expecting compaction
		//g.Expect(finalSize < initialSize).To(BeTrue())
		g.Expect(finalSize).To(BeNumerically("<", initialSize))

		// Cleanup the rest of the entries before the next iteration
		deleteEntries(ctx, g, client, numDelEntries, numAddEntries)
	}
}
