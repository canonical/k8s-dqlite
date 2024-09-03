package test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestList is the unit test for List operation.
func TestList(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			g := NewWithT(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKineServer(ctx, t, &kineOptions{backendType: backendType})

			keys := []string{"/key/5", "/key/4", "/key/3", "/key/2", "/key/1"}
			for _, key := range keys {
				createKey(ctx, g, kine.client, key, "value")
			}

			t.Run("ListAll", func(t *testing.T) {
				g := NewWithT(t)

				resp, err := kine.client.Get(ctx, "/key", clientv3.WithPrefix())

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(5))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))
				g.Expect(resp.Kvs[2].Key).To(Equal([]byte("/key/3")))
				g.Expect(resp.Kvs[3].Key).To(Equal([]byte("/key/4")))
				g.Expect(resp.Kvs[4].Key).To(Equal([]byte("/key/5")))
			})

			t.Run("ListAllLimit", func(t *testing.T) {
				g := NewWithT(t)

				resp, err := kine.client.Get(ctx, "/key", clientv3.WithPrefix(), clientv3.WithLimit(2))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.More).To(BeTrue())
				g.Expect(resp.Count).To(Equal(int64(5)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))

				rev := resp.Header.Revision

				// Inspired from https://github.com/kubernetes/kubernetes/blob/3f4d3b67682335db510f85deb65b322127a3a0a1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L788-L793
				// Key is "last_key" + "\x00", and we use the prefix range end
				resp, err = kine.client.Get(ctx, "/key/2\x00",
					clientv3.WithRange(clientv3.GetPrefixRangeEnd("/key")),
					clientv3.WithLimit(2),
					clientv3.WithRev(rev))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.More).To(BeTrue())
				g.Expect(resp.Count).To(Equal(int64(3)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/3")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/4")))

				rev = resp.Header.Revision

				resp, err = kine.client.Get(ctx, "/key/4\x00",
					clientv3.WithRange(clientv3.GetPrefixRangeEnd("/key")),
					clientv3.WithLimit(2),
					clientv3.WithRev(rev))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.More).To(BeFalse())
				g.Expect(resp.Count).To(Equal(int64(1)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/5")))
			})

			t.Run("ListPrefix", func(t *testing.T) {
				g := NewWithT(t)

				keys := []string{"key/sub/2", "key/sub/1", "key/other/1"}
				for _, key := range keys {
					createKey(ctx, g, kine.client, key, "value")
				}

				resp, err := kine.client.Get(ctx, "/key", clientv3.WithPrefix())

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(5))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))
				g.Expect(resp.Kvs[2].Key).To(Equal([]byte("/key/3")))
				g.Expect(resp.Kvs[3].Key).To(Equal([]byte("/key/4")))
				g.Expect(resp.Kvs[4].Key).To(Equal([]byte("/key/5")))

				resp, err = kine.client.Get(ctx, "key/sub", clientv3.WithPrefix())

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/sub/1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("key/sub/2")))

				resp, err = kine.client.Get(ctx, "key/other", clientv3.WithPrefix())

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/other/1")))
			})

			t.Run("ListRange", func(t *testing.T) {
				g := NewWithT(t)

				resp, err := kine.client.Get(ctx, "/key/1", clientv3.WithRange(""))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
			})

			t.Run("ListRevision", func(t *testing.T) {
				g := NewWithT(t)

				const key = "/revkey/1"
				createRev := createKey(ctx, g, kine.client, key, "value")

				const updates = 50
				for i, rev := 0, createRev; i < updates; i++ {
					rev = updateRev(ctx, g, kine.client, key, rev, fmt.Sprintf("val-%d", i))
				}

				t.Run("NoRevision", func(t *testing.T) {
					g := NewWithT(t)

					resp, err := kine.client.Get(ctx, "/revkey/", clientv3.WithPrefix())
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(createRev + updates)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})

				t.Run("OldRevision", func(t *testing.T) {
					g := NewWithT(t)

					resp, err := kine.client.Get(ctx, "/revkey/", clientv3.WithPrefix(), clientv3.WithRev(createRev+30))
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(createRev + 30)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})

				t.Run("LaterRevision", func(t *testing.T) {
					g := NewWithT(t)

					resp, err := kine.client.Get(ctx, "/revkey/", clientv3.WithPrefix(), clientv3.WithRev(createRev+100))
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(createRev + updates)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})
			})
		})
	}
}

func BenchmarkList(b *testing.B) {
	setup := func(ctx context.Context, tx *sql.Tx, payloadSize, n int) error {
		if err := insertMany(ctx, tx, "key", payloadSize, n); err != nil {
			return err
		}
		if err := updateMany(ctx, tx, "key", payloadSize, n/2); err != nil {
			return err
		}
		if err := deleteMany(ctx, tx, "key", n/2); err != nil {
			return err
		}
		return nil
	}
	backends := []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend}
	for _, backendType := range backends {
		payloads := []struct {
			name string
			size int
		}{{
			name: "tiny",
			size: 100,
		}, {
			name: "fits-in-page",
			size: 1000,
		}, {
			name: "overflows-page",
			size: 5000,
		}}
		for _, payload := range payloads {
			b.Run(fmt.Sprintf("%s-%s", backendType, payload.name), func(b *testing.B) {
				b.Run("all", func(b *testing.B) {
					g := NewWithT(b)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					kine := newKineServer(ctx, b, &kineOptions{
						backendType: backendType,
						setup: func(ctx context.Context, tx *sql.Tx) error {
							return setup(ctx, tx, payload.size, b.N)
						},
					})

					kine.ResetMetrics()
					b.StartTimer()
					resp, err := kine.client.Get(ctx, "key/", clientv3.WithPrefix())

					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen((b.N + 1) / 2))
					b.StopTimer()
					kine.ReportMetrics(b)
				})

				b.Run("pagination", func(b *testing.B) {
					g := NewWithT(b)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					kine := newKineServer(ctx, b, &kineOptions{
						backendType: backendType,
						setup: func(ctx context.Context, tx *sql.Tx) error {
							return setup(ctx, tx, payload.size, b.N)
						},
					})

					kine.ResetMetrics()
					b.StartTimer()
					nextKey := "key/"
					endRange := clientv3.GetPrefixRangeEnd(nextKey)
					count := 0
					for more := true; more; {
						resp, err := kine.client.Get(ctx,
							nextKey,
							clientv3.WithRange(endRange),
							clientv3.WithLimit(int64(b.N)/10),
						)
						g.Expect(err).To(BeNil())

						more = resp.More
						count += len(resp.Kvs)
						nextKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x01"
					}

					g.Expect(count).To(Equal((b.N + 1) / 2))
					b.StopTimer()
					kine.ReportMetrics(b)
				})
			})
		}
	}
}
