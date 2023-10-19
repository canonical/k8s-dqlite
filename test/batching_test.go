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
	client, _ := newKine(ctx, t, "batching-interval=200ms", "batching-max-queries=100", "batching-enabled=true")

	g := NewWithT(t)
	var wg WaitGroupCount

	writer := func(first int, last int) {
		defer wg.Done()
		for i := first; i < last; i++ {
			key := fmt.Sprintf("Key-%d", i)
			new_value := fmt.Sprintf("Value-%d", i)

			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, new_value)).
				Commit()
			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
		}
	}

	maxWrites := 1000
	writers := 100
	write_entries := maxWrites / writers
	wg.Add(writers)

	start := time.Now()
	fmt.Println("Start writing")
	for i := 0; i < writers; i++ {
		go writer(i*write_entries, (i+1)*write_entries)
	}

	go func() {
		for {

			count := wg.GetCount()
			fmt.Printf("WG Count: %d\n", count)
			if count == 0 {
				return
			}
			wg.GetCount()
			time.Sleep(time.Second)

		}
	}()
	wg.Wait()

	fmt.Println("Validating writes")
	for i := 0; i < maxWrites; i++ {
		key := fmt.Sprintf("Key-%d", i)
		expectedValue := fmt.Sprintf("Value-%d", i)
		resp, err := client.Get(ctx, key, clientv3.WithRange(""))
		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(HaveLen(1))
		g.Expect(resp.Kvs[0].Key).To(Equal([]byte(key)))
		g.Expect(resp.Kvs[0].Value).To(Equal([]byte(expectedValue)))
	}

	duration := time.Since(start)

	t.Logf("Executed 1000 queries in %.2f seconds\n", duration.Seconds())
}
