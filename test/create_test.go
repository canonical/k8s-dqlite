package test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestCreate is unit testing for the create operation.
func TestCreate(t *testing.T) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			g := NewWithT(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			server := newK8sDqliteServer(ctx, t, &k8sDqliteConfig{backendType: backendType})

			createKey(ctx, g, server.client, "testKey", "testValue")
			resp, err := server.client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision("testKey"), "=", 0)).
				Then(clientv3.OpPut("testKey", "testValue2")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeFalse())
		})
	}
}

// BenchmarkCreate is a benchmark for the Create operation.
func BenchmarkCreate(b *testing.B) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		for _, workers := range []int{1, 4, 16, 64, 128} {
			b.Run(fmt.Sprintf("%s/%d-workers", backendType, workers), func(b *testing.B) {
				b.StopTimer()
				g := NewWithT(b)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server := newK8sDqliteServer(ctx, b, &k8sDqliteConfig{backendType: backendType})
				wg := &sync.WaitGroup{}
				run := func(start int) {
					defer wg.Done()
					for i := start; i < b.N; i += workers {
						key := fmt.Sprintf("key-%d", i)
						value := fmt.Sprintf("value-%d", i)
						createKey(ctx, g, server.client, key, value)
					}
				}

				server.ResetMetrics()
				b.StartTimer()
				wg.Add(workers)
				for worker := 0; worker < workers; worker++ {
					go run(worker)
				}
				wg.Wait()
				server.ReportMetrics(b)
			})
		}
	}
}

func createKey(ctx context.Context, g Gomega, client *clientv3.Client, key string, value string, opts ...clientv3.OpOption) int64 {
	resp, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, value, opts...)).
		Commit()

	g.Expect(err).To(BeNil())
	g.Expect(resp.Succeeded).To(BeTrue())
	g.Expect(resp.Responses).To(HaveLen(1))
	g.Expect(resp.Responses[0].GetResponsePut()).NotTo(BeNil())
	return resp.Responses[0].GetResponsePut().Header.Revision
}
