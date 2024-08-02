package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestAdmissionControl puts heavy load on kine and expects that some requests are denied
// by the admission control.
func TestAdmissionControl(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			const writeLimit = 10

			g := NewWithT(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{
				backendType: backendType,
				endpointParameters: []string{
					"admission-control-policy=limit",
					fmt.Sprintf("admission-control-policy-limit-max-concurrent-txn=%d", writeLimit),
					"admission-control-only-write-queries=true",
				},
				setup: func(ctx context.Context, tx *sql.Tx) error {
					if err := insertMany(ctx, tx, "Key", 100, 1000); err != nil {
						return err
					}
					return nil
				},
			})

			var wg sync.WaitGroup

			var numSuccessfulWriterTxn = atomic.Uint64{}
			var numSuccessfulReaderTxn = atomic.Uint64{}

			read := func(firstKeyNum, lastKeyNum int) {
				defer wg.Done()
				for i := firstKeyNum; i < lastKeyNum; i++ {
					key := fmt.Sprintf("Key/%d", i+1)
					_, err := kine.client.Get(ctx, key, clientv3.WithRange(""))
					if err == nil {
						numSuccessfulReaderTxn.Add(1)
					}
				}
			}

			write := func(firstKeyNum, lastKeyNum int) {
				defer wg.Done()
				for i := firstKeyNum; i < lastKeyNum; i++ {
					key := fmt.Sprintf("Key/%d", i+1)
					new_value := fmt.Sprintf("New-Value-%d", i+1)
					resp, err := kine.client.Get(ctx, key, clientv3.WithRange(""))
					if err != nil || len(resp.Kvs) == 0 {
						t.Logf("Could not get %s\n", key)
						continue
					}
					lastModRev := resp.Kvs[0].ModRevision
					put_resp, err := kine.client.Txn(ctx).
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

			readers := writeLimit * 4
			read_entries := 20

			writers := writeLimit * 4
			write_entries := 20

			wg.Add(readers + writers)

			start := time.Now()
			for i := 0; i < readers; i++ {
				go read(i*read_entries, (i+1)*read_entries)
			}
			for i := 0; i < writers; i++ {
				go write(i*write_entries, (i+1)*write_entries)
			}

			wg.Wait()
			duration := time.Since(start)

			t.Logf("Executed 1000 queries in %.2f seconds\n", duration.Seconds())
			// It is expected that some queries are denied by the admission control due to the load.
			g.Expect(numSuccessfulWriterTxn.Load()).To(BeNumerically("<", writers*write_entries))

			// read queries should be ignored by the admission control
			g.Expect(numSuccessfulReaderTxn.Load()).To(BeNumerically("==", readers*read_entries))
		})
	}
}
