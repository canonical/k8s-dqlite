package test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/v3/app"
	"github.com/canonical/go-dqlite/v3/driver"
	"github.com/canonical/k8s-dqlite/pkg/instrument"
	. "github.com/onsi/gomega"
)

// openBusyTimeoutDB starts a dqlite node with the given options and returns a
// database with an empty table `t`.
func openBusyTimeoutDB(ctx context.Context, t *testing.T, options ...app.Option) *sql.DB {
	dir := t.TempDir()
	listener := instrument.NewListener("unix", path.Join(dir, "dqlite.sock"))
	if err := listener.Listen(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		listener.Close()
		if err := listener.Err(); err != nil {
			t.Error(err)
		}
	})

	dqliteApp := newDqliteApp(ctx, t, dir, listener, options...)
	db, err := dqliteApp.Open(ctx, "busy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.ExecContext(ctx, "CREATE TABLE t (n INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	return db
}

// holdWriteTransaction opens a dedicated connection and leaves a write
// transaction open on it, so that writes on other connections collide with it.
func holdWriteTransaction(ctx context.Context, t *testing.T, g *WithT, db *sql.DB) *sql.Tx {
	conn, err := db.Conn(ctx)
	g.Expect(err).ToNot(HaveOccurred())
	t.Cleanup(func() { conn.Close() })
	tx, err := conn.BeginTx(ctx, nil)
	g.Expect(err).ToNot(HaveOccurred())
	_, err = tx.ExecContext(ctx, "INSERT INTO t (n) VALUES (1)")
	g.Expect(err).ToNot(HaveOccurred())
	return tx
}

func isBusy(err error) bool {
	var e driver.Error
	return errors.As(err, &e) && e.Code == driver.ErrBusy
}

func countRows(ctx context.Context, g *WithT, db *sql.DB) int {
	var n int
	g.Expect(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM t").Scan(&n)).To(Succeed())
	return n
}

// TestBusyTimeout verifies how dqlite treats a write that collides with
// another connection's write transaction, with and without a busy timeout.
func TestBusyTimeout(t *testing.T) {
	t.Run("DefaultFailsImmediately", func(t *testing.T) {
		g := NewWithT(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := openBusyTimeoutDB(ctx, t)
		tx := holdWriteTransaction(ctx, t, g, db)
		defer tx.Rollback()

		start := time.Now()
		_, err := db.ExecContext(ctx, "INSERT INTO t (n) VALUES (2)")
		g.Expect(isBusy(err)).To(BeTrue(), "expected SQLITE_BUSY, got %v", err)
		// Without a busy timeout the write is rejected on the next event-loop
		// tick rather than after any wait.
		g.Expect(time.Since(start)).To(BeNumerically("<", time.Second))
	})

	t.Run("WaitsForTimeoutBeforeFailing", func(t *testing.T) {
		g := NewWithT(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const busyTimeout = 300 * time.Millisecond
		db := openBusyTimeoutDB(ctx, t, app.WithBusyTimeout(busyTimeout))
		tx := holdWriteTransaction(ctx, t, g, db)
		defer tx.Rollback()

		start := time.Now()
		_, err := db.ExecContext(ctx, "INSERT INTO t (n) VALUES (2)")
		elapsed := time.Since(start)
		g.Expect(isBusy(err)).To(BeTrue(), "expected SQLITE_BUSY, got %v", err)
		g.Expect(elapsed).To(BeNumerically(">=", busyTimeout))
		g.Expect(elapsed).To(BeNumerically("<", 10*busyTimeout))
	})

	t.Run("QueuedWriteSucceedsWhenHolderCommits", func(t *testing.T) {
		g := NewWithT(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		db := openBusyTimeoutDB(ctx, t, app.WithBusyTimeout(10*time.Second))
		tx := holdWriteTransaction(ctx, t, g, db)

		result := make(chan error, 1)
		go func() {
			_, err := db.ExecContext(ctx, "INSERT INTO t (n) VALUES (2)")
			result <- err
		}()

		g.Expect(tx.Commit()).To(Succeed())
		g.Expect(<-result).ToNot(HaveOccurred())
		g.Expect(countRows(ctx, g, db)).To(Equal(2))
	})

	t.Run("ConcurrentWritersAllSucceed", func(t *testing.T) {
		g := NewWithT(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const writers, writesPerWriter = 8, 50
		db := openBusyTimeoutDB(ctx, t, app.WithBusyTimeout(10*time.Second))

		var wg sync.WaitGroup
		errs := make(chan error, writers*writesPerWriter)
		for w := 0; w < writers; w++ {
			conn, err := db.Conn(ctx)
			g.Expect(err).ToNot(HaveOccurred())
			defer conn.Close()

			wg.Add(1)
			go func(w int, conn *sql.Conn) {
				defer wg.Done()
				for i := 0; i < writesPerWriter; i++ {
					if _, err := conn.ExecContext(ctx, "INSERT INTO t (n) VALUES (?)", w*writesPerWriter+i); err != nil {
						errs <- fmt.Errorf("writer %d write %d: %w", w, i, err)
					}
				}
			}(w, conn)
		}
		wg.Wait()
		close(errs)

		for err := range errs {
			t.Error(err)
		}
		g.Expect(countRows(ctx, g, db)).To(Equal(writers * writesPerWriter))
	})
}
