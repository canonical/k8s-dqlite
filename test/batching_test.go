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

type WaitGroupCount struct {
	sync.WaitGroup
	count int64
}

func (wg *WaitGroupCount) Add(delta int) {
	atomic.AddInt64(&wg.count, int64(delta))
	wg.WaitGroup.Add(delta)
}

func (wg *WaitGroupCount) Done() {
	atomic.AddInt64(&wg.count, -1)
	wg.WaitGroup.Done()
}

func (wg *WaitGroupCount) GetCount() int {
	return int(atomic.LoadInt64(&wg.count))
}

func TestWriteBatching(t *testing.T) {
	ctx := context.Background()
	g := NewWithT(t)

	t.Run("PerformanceComparison", func(t *testing.T) {
		numWrites := 10000
		writers := 100

		t.Log("Run without batching")
		durationWithoutBatching := perfTest(
			ctx, t,
			numWrites, writers,
			"batching-enabled=false",
		)

		t.Log("Run with batching")
		durationWithBatching := perfTest(
			ctx, t,
			numWrites, writers,
			"batching-interval=200ms", "batching-max-queries=100", "batching-enabled=true",
		)

		t.Logf("With Batching: %v, without Batching: %v", durationWithBatching, durationWithoutBatching)
		g.Expect(durationWithBatching).To(BeNumerically("<", durationWithoutBatching))
	})
}

func perfTest(ctx context.Context, t *testing.T, numWrites int, writers int, batchingConfig ...string) time.Duration {

	client, _ := newKine(ctx, t, batchingConfig...)
	var wg WaitGroupCount
	g := NewWithT(t)

	writer := func(first int, last int) {
		defer wg.Done()
		for i := first; i < last; i++ {
			for {
				key := fmt.Sprintf("Key-%d", i)
				new_value := fmt.Sprintf("Value-%d", i)

				resp, err := client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
					Then(clientv3.OpPut(key, new_value)).
					Commit()

				if err == nil && resp.Succeeded {
					break
				}
			}
		}
	}

	write_entries := numWrites / writers
	wg.Add(writers)

	start := time.Now()
	for i := 0; i < writers; i++ {
		go writer(i*write_entries, (i+1)*write_entries)
	}
	wg.Wait()
	duration := time.Since(start)

	// Validating writes (excluded from timing ofc)
	for i := 0; i < numWrites; i++ {
		key := fmt.Sprintf("Key-%d", i)
		expectedValue := fmt.Sprintf("Value-%d", i)
		resp, err := client.Get(ctx, key, clientv3.WithRange(""))
		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(HaveLen(1))
		g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
		g.Expect(resp.Kvs[0].Value).To(Equal([]byte(expectedValue)))
	}

	return duration
}
