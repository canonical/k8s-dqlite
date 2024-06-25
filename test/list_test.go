package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestList is the unit test for List operation.
func TestList(t *testing.T) {
	ctx := context.Background()
	client, _ := newKine(ctx, t)

	t.Run("ListSuccess", func(t *testing.T) {
		g := NewWithT(t)

		// Create some keys
		keys := shuffleList([]string{"/key/1", "/key/2", "/key/3", "/key/4", "/key/5"})
		for _, key := range keys {
			resp, err := client.Txn(ctx).
				If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
				Then(clientv3.OpPut(key, "value")).
				Commit()

			g.Expect(err).To(BeNil())
			g.Expect(resp.Succeeded).To(BeTrue())
			g.Expect(resp.Header.Revision).ToNot(BeZero())
		}

		t.Run("ListAll", func(t *testing.T) {
			g := NewWithT(t)
			// Get a list of all the keys
			resp, err := client.Get(ctx, "/key", clientv3.WithPrefix())

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
			var revision int64
			t.Run("FirstPage", func(t *testing.T) {
				g := NewWithT(t)

				resp, err := client.Get(ctx, "/key", clientv3.WithPrefix(), clientv3.WithLimit(2))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.More).To(BeTrue())
				g.Expect(resp.Count).To(Equal(int64(5)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))

				revision = resp.Header.Revision
			})

			t.Run("SecondPage", func(t *testing.T) {
				g := NewWithT(t)

				// Inspired from https://github.com/kubernetes/kubernetes/blob/3f4d3b67682335db510f85deb65b322127a3a0a1/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L788-L793
				// Key is "last_key" + "\x00", and we use the prefix range end
				resp, err := client.Get(ctx, "/key/2\x00", clientv3.WithRange(clientv3.GetPrefixRangeEnd("/key")), clientv3.WithLimit(2), clientv3.WithRev(revision))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(2))
				g.Expect(resp.More).To(BeTrue())
				g.Expect(resp.Count).To(Equal(int64(3)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/3")))
				g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/4")))

				revision = resp.Header.Revision
			})

			t.Run("ThirdPage", func(t *testing.T) {
				g := NewWithT(t)

				// Get a list of all the keys
				resp, err := client.Get(ctx, "/key/4\x00", clientv3.WithRange(clientv3.GetPrefixRangeEnd("/key")), clientv3.WithLimit(2), clientv3.WithRev(revision))

				g.Expect(err).To(BeNil())
				g.Expect(resp.Kvs).To(HaveLen(1))
				g.Expect(resp.More).To(BeFalse())
				g.Expect(resp.Count).To(Equal(int64(1)))
				g.Expect(resp.Header.Revision).ToNot(BeZero())
				g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/5")))
			})
		})

		t.Run("ListPrefix", func(t *testing.T) {
			g := NewWithT(t)
			// Create some keys
			keys := []string{"key/sub/2", "key/sub/1", "key/other/1"}
			for _, key := range keys {
				resp, err := client.Txn(ctx).
					If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
					Then(clientv3.OpPut(key, "value")).
					Commit()

				g.Expect(err).To(BeNil())
				g.Expect(resp.Succeeded).To(BeTrue())
				g.Expect(resp.Header.Revision).ToNot(BeZero())
			}

			// Get a list of all the keys sice they have '/key' prefix
			resp, err := client.Get(ctx, "/key", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(5))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("/key/2")))
			g.Expect(resp.Kvs[2].Key).To(Equal([]byte("/key/3")))
			g.Expect(resp.Kvs[3].Key).To(Equal([]byte("/key/4")))
			g.Expect(resp.Kvs[4].Key).To(Equal([]byte("/key/5")))

			// Get a list of all the keys sice they have '/key/sub' prefix
			resp, err = client.Get(ctx, "key/sub", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(2))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/sub/1")))
			g.Expect(resp.Kvs[1].Key).To(Equal([]byte("key/sub/2")))

			// Get a list of all the keys sice they have '/key/other' prefix
			resp, err = client.Get(ctx, "key/other", clientv3.WithPrefix())

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("key/other/1")))
		})

		t.Run("ListRange", func(t *testing.T) {
			g := NewWithT(t)

			// Get a list of with key/1, as only key/1 falls within the specified range.
			resp, err := client.Get(ctx, "/key/1", clientv3.WithRange(""))

			g.Expect(err).To(BeNil())
			g.Expect(resp.Kvs).To(HaveLen(1))
			g.Expect(resp.Header.Revision).ToNot(BeZero())
			g.Expect(resp.Kvs[0].Key).To(Equal([]byte("/key/1")))
		})

		t.Run("ListRevision", func(t *testing.T) {
			t.Run("Create", func(t *testing.T) {
				g := NewWithT(t)

				// Create some keys
				keys := []string{"/revkey/1"}
				for _, key := range keys {
					resp, err := client.Txn(ctx).
						If(clientv3.Compare(clientv3.ModRevision(key), "=", 0)).
						Then(clientv3.OpPut(key, "value")).
						Commit()

					g.Expect(err).To(BeNil())
					g.Expect(resp.Succeeded).To(BeTrue())
					g.Expect(resp.Header.Revision).ToNot(BeZero())
				}
			})

			t.Run("Update", func(t *testing.T) {
				g := NewWithT(t)
				var rev int64
				for rev < 50 {
					get, err := client.Get(ctx, "/revkey/1", clientv3.WithRange(""))
					g.Expect(err).To(BeNil())
					g.Expect(get.Kvs).To(HaveLen(1))
					rev = get.Kvs[0].ModRevision

					update, err := client.Txn(ctx).
						If(clientv3.Compare(clientv3.ModRevision("/revkey/1"), "=", rev)).
						Then(clientv3.OpPut("/revkey/1", fmt.Sprintf("val-%d", rev))).
						Else(clientv3.OpGet("/revkey/1", clientv3.WithRange(""))).
						Commit()

					g.Expect(err).To(BeNil())
					g.Expect(update.Succeeded).To(BeTrue())
				}
			})

			t.Run("List", func(t *testing.T) {
				t.Run("NoRevision", func(t *testing.T) {
					g := NewWithT(t)
					resp, err := client.Get(ctx, "/revkey/", clientv3.WithPrefix())
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(51)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})

				t.Run("OldRevision", func(t *testing.T) {
					g := NewWithT(t)
					resp, err := client.Get(ctx, "/revkey/", clientv3.WithPrefix(), clientv3.WithRev(30))
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(30)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})
				t.Run("LaterRevision", func(t *testing.T) {
					g := NewWithT(t)
					resp, err := client.Get(ctx, "/revkey/", clientv3.WithPrefix(), clientv3.WithRev(100))
					g.Expect(err).To(BeNil())
					g.Expect(resp.Kvs).To(HaveLen(1))
					g.Expect(resp.Kvs[0].ModRevision).To(Equal(int64(51)))
					g.Expect(resp.Count).To(Equal(int64(1)))
				})
			})
		})
	})
}

// BenchmarkList is a benchmark for the Get operation.
func BenchmarkList(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()
	client, _ := newKine(ctx, b)
	g := NewWithT(b)

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key/%d", i)
		createKey(ctx, g, client, key, "benchValue")
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(ctx, "key/", clientv3.WithPrefix())

		g.Expect(err).To(BeNil())
		g.Expect(resp.Kvs).To(HaveLen(b.N))
	}
}

func shuffleList[T any](vals []T) []T {
	if len(vals) == 0 {
		return vals
	}

	perm := rand.Perm(len(vals))
	shuffled := make([]T, 0, len(vals))
	for _, i := range perm {
		shuffled = append(shuffled, vals[perm[i]])
	}

	return shuffled
}
