package database_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/database"
)

func TestBatchedSequentialLoad(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewBatched(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	for i := 0; i < 100; i++ {
		_, err := db.ExecContext(ctx, "query 1")
		if err != nil {
			t.Error(err)
		}
	}
	if trans := driver.trans.Load(); trans != 0 {
		t.Errorf("unexpected number of transaction: want 0, got %d", trans)
	}
}

func TestBatchedParallelLoad(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewBatched(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	wg := &sync.WaitGroup{}
	wg.Add(1000)
	for i := 0; i < 1000; i++ {
		go func() {
			defer wg.Done()
			_, err := db.ExecContext(ctx, "query 1")
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	trans := driver.trans.Load()
	if trans == 0 {
		t.Error("unexpected number of transaction: want >0, got 0")
	}
}

func TestBatchedClosed(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewBatched(sql.OpenDB(&testConnector{driver: driver}))
	db.Close()

	_, err := db.ExecContext(ctx, "query 1")
	if err == nil {
		t.Error("closed connection did not fail")
	}
}

func TestBatchedCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	driver := &testDriver{
		t: t,
	}
	db := database.NewBatched(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	_, err := db.ExecContext(ctx, "query 1")
	if err == nil {
		t.Error("closed connection did not fail")
	}
}
