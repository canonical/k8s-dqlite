package test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAdmissionControl puts heavy load on kine and expects that some requests are denied
// by the admission control.
func TestAdmissionControl(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, _ := newKine(ctx, t, "admission-control-policy=limit", "admission-control-policy-limit-max-concurrent-txn=600", "admission-control-only-write-queries=true")

	// create a key space of 1000 items
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("Key-%d", i)
		value := fmt.Sprintf("Value-%d", i)
		createKey(ctx, g, client, key, value)
	}

	var wg sync.WaitGroup

	var numSuccessfulWriterTxn = atomic.Uint64{}
	var numSuccessfulReaderTxn = atomic.Uint64{}

	reader := func(first int, last int) {
		defer wg.Done()
		for i := first; i < last; i++ {
			key := fmt.Sprintf("Key-%d", i)
			_, err := client.Get(ctx, key, clientv3.WithRange(""))
			if err == nil {
				numSuccessfulReaderTxn.Add(1)
			}
		}
	}

	writer := func(first int, last int) {
		defer wg.Done()
		for i := first; i < last; i++ {
			key := fmt.Sprintf("Key-%d", i)
			new_value := fmt.Sprintf("New-Value-%d", i)
			resp, err := client.Get(ctx, key, clientv3.WithRange(""))
			if err != nil || len(resp.Kvs) == 0 {
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
				numSuccessfulWriterTxn.Add(1)
				break
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
	// It is expected that some queries are denied by the admission control due to the load.
	g.Expect(numSuccessfulWriterTxn.Load()).To(BeNumerically("<", writers*writers_replication*write_entries))

	// read queries should be ignored by the admission control
	g.Expect(numSuccessfulReaderTxn.Load()).To(BeNumerically("==", readers*readers_replication*read_entries))
}
