package test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// BenchmarkGet is a benchmark for the Get operation.
func TestMixed(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)
	g := NewWithT(t)

	// create a key space of 1000 items
	{
		for i := 0; i < 1000; i++ {
			key := fmt.Sprintf("Key-%d", i)
			value := fmt.Sprintf("Value-%d", i)
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, value)).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}
	}

	t.Run("LatestRevision", func(t *testing.T) {
		g := NewWithT(t)
		var wg sync.WaitGroup

		reader := func(first int, last int) {
			defer wg.Done()
			for i := first; i < last; i++ {
				key := fmt.Sprintf("Key-%d", i)
				resp, err := client.Get(ctx, key, clientv3.WithRange(""))
				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
			}
		}

		writer := func(first int, last int) {
			defer wg.Done()
			for i := first; i < last; i++ {
				key := fmt.Sprintf("Key-%d", i)
				new_value := fmt.Sprintf("New-Value-%d", i)
				for {
					resp, err := client.Get(ctx, key, clientv3.WithRange(""))
					if err != nil {
						t.Logf("Could not get %s\n", key)
						continue
					}
					lastModRev := resp.Kvs[0].ModRevision
					put_resp, err := client.Txn(ctx).
						If(clientv3.Compare(clientv3.ModRevision(key), "=", lastModRev)).
						Then(clientv3.OpPut(key, new_value)).
						Else(clientv3.OpGet(key, clientv3.WithRange(""))).
						Commit()

					if err == nil && put_resp.Succeeded == true {
						break
					}
				}
			}
		}

		readers := 50
		readers_replication := 3
		read_entries := 1000 / readers
		writers := 500
		writers_replication := 10
		write_entries := 1000 / writers
		wg.Add(readers*readers_replication + writers*writers_replication)

		start := time.Now()
		for i := 0; i < readers; i++ {
			for j := 0; j < readers_replication; j++ {
				go reader(i*read_entries, (i+1)*read_entries)
			}
		}
		for i := 0; i < writers; i++ {
			for j := 0; j < writers_replication; j++ {
				go writer(i*write_entries, (i+1)*write_entries)
			}
		}

		wg.Wait()
		duration := time.Since(start)

		t.Logf("Executed 1000 queries in %.2f seconds\n", duration.Seconds())
	})
}
