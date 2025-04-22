package test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/drivers/sqlite"
	. "github.com/onsi/gomega"
)

func TestCompaction(t *testing.T) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			t.Run("SmallDatabaseDeleteEntry", func(t *testing.T) {
				g := NewWithT(t)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server := newK8sDqliteServer(ctx, t, &k8sDqliteConfig{
					backendType: backendType,
					setup: func(ctx context.Context, tx *sql.Tx) error {
						if _, err := insertMany(ctx, tx, "key", 100, 2); err != nil {
							return err
						}
						if _, err := updateMany(ctx, tx, "key", 100, 1); err != nil {
							return err
						}
						if _, err := deleteMany(ctx, tx, "key", 1); err != nil {
							return err
						}
						return nil
					},
				})

				initialSize, err := server.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				err = server.backend.DoCompact(ctx)
				g.Expect(err).To(BeNil())

				finalSize, err := server.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())
				g.Expect(finalSize).To(BeNumerically("==", initialSize)) // Expecting no compaction.
			})

			t.Run("LargeDatabaseDeleteFivePercent", func(t *testing.T) {
				g := NewWithT(t)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server := newK8sDqliteServer(ctx, t, &k8sDqliteConfig{
					backendType: backendType,
					setup: func(ctx context.Context, tx *sql.Tx) error {
						if _, err := insertMany(ctx, tx, "key", 100, 10_000); err != nil {
							return err
						}
						if _, err := updateMany(ctx, tx, "key", 100, 500); err != nil {
							return err
						}
						if _, err := deleteMany(ctx, tx, "key", 500); err != nil {
							return err
						}
						return nil
					},
				})

				initialSize, err := server.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				err = server.backend.DoCompact(ctx)
				g.Expect(err).To(BeNil())

				// Expect compaction to reduce the size.
				finalSize, err := server.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())
				g.Expect(finalSize).To(BeNumerically("<", initialSize))

				// Expect for keys to still be there.
				rev, count, err := server.backend.Count(ctx, []byte("key/"), []byte("key0"), 0)
				g.Expect(err).To(BeNil())
				g.Expect(count).To(Equal(int64(10_000 - 500)))

				// Expect old revisions not to be there anymore.
				_, _, err = server.backend.List(ctx, []byte("key/"), []byte("key0"), 0, rev-400)
				g.Expect(err).To(Not(BeNil()))
			})
		})
	}
}

func BenchmarkCompaction(b *testing.B) {
	for _, backendType := range []string{SQLiteBackend, DQLiteBackend} {
		b.Run(backendType, func(b *testing.B) {
			b.StopTimer()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := newK8sDqliteServer(ctx, b, &k8sDqliteConfig{
				backendType: backendType,
				setup: func(ctx context.Context, tx *sql.Tx) error {
					// Make sure there are enough rows deleted to have
					// b.N rows to compact.
					delCount := b.N + sqlite.SupersededCount

					// Also, make sure there are uncollectable data, so
					// that the deleted rows are about 5% of the total.
					addCount := delCount * 20

					if _, err := insertMany(ctx, tx, "key", 100, addCount); err != nil {
						return err
					}
					if _, err := deleteMany(ctx, tx, "key", delCount); err != nil {
						return err
					}
					return nil
				},
			})
			server.ResetMetrics()
			b.StartTimer()
			if err := server.backend.DoCompact(ctx); err != nil {
				b.Fatal(err)
			}
			server.ReportMetrics(b)
		})
	}
}
