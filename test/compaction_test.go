package test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/kine/logstructured/sqllog"
	. "github.com/onsi/gomega"
)

func TestCompaction(t *testing.T) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		t.Run(backendType, func(t *testing.T) {
			t.Run("SmallDatabaseDeleteEntry", func(t *testing.T) {
				g := NewWithT(t)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				kine := newKine(ctx, t, &kineOptions{
					backendType: backendType,
					setup: func(db *sql.DB) error {
						return setupScenario(ctx, db, "testkey", 2, 1, 1)
					},
				})

				initialSize, err := kine.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				err = kine.backend.DoCompact(ctx)
				g.Expect(err).To(BeNil())

				finalSize, err := kine.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				// Expecting no compaction
				g.Expect(finalSize).To(BeNumerically("==", initialSize))
			})

			t.Run("LargeDatabaseDeleteFivePercent", func(t *testing.T) {
				g := NewWithT(t)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				kine := newKine(ctx, t, &kineOptions{
					backendType: backendType,
					setup: func(db *sql.DB) error {
						return setupScenario(ctx, db, "testkey", 10_000, 500, 500)
					},
				})

				initialSize, err := kine.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				err = kine.backend.DoCompact(ctx)
				g.Expect(err).To(BeNil())

				finalSize, err := kine.backend.DbSize(ctx)
				g.Expect(err).To(BeNil())

				// Expecting compaction
				g.Expect(finalSize).To(BeNumerically("<", initialSize))
			})
		})
	}
}

func BenchmarkCompaction(b *testing.B) {
	for _, backendType := range []string{endpoint.SQLiteBackend, endpoint.DQLiteBackend} {
		b.Run(backendType, func(b *testing.B) {
			b.StopTimer()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			kine := newKine(ctx, b, &kineOptions{
				backendType: backendType,
				setup: func(db *sql.DB) error {
					// Make sure there's enough rows deleted to have
					// b.N rows to compact.
					delCount := b.N + sqllog.SupersededCount

					// Also, make sure there's some uncollectable data, so
					// that the deleted rows are about 5% of the total.
					addCount := delCount * 20

					return setupScenario(ctx, db, "testkey", addCount, 0, delCount)
				},
			})

			kine.ResetMetrics()
			b.StartTimer()
			if err := kine.backend.DoCompact(ctx); err != nil {
				b.Fatal(err)
			}
			kine.ReportMetrics(b)
		})
	}
}
