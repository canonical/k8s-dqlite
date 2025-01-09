package database_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/database"
)

func TestPreparedPrepare(t *testing.T) {
	const expectedQuery = "test query"

	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewPrepared(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	stmt1, err := db.PrepareContext(ctx, expectedQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt1.Close()

	stmt2, err := db.PrepareContext(ctx, expectedQuery)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt2.Close()

	if stmts := driver.stmts.Load(); stmts != 2 {
		t.Errorf("invalid open statements: want 1, got %d", stmts)
	}
}

func TestPreparedQuery(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewPrepared(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	rows, err := db.QueryContext(ctx, "query 1")
	if err != nil {
		t.Error(err)
	}
	rows.Close()

	if stmts := driver.stmts.Load(); stmts != 1 {
		t.Errorf("unexpected number of open statements: want %d, got %d", 1, stmts)
	}

	rows, err = db.QueryContext(ctx, "query 2")
	if err != nil {
		t.Error(err)
	}
	rows.Close()

	if stmts := driver.stmts.Load(); stmts != 2 {
		t.Errorf("unexpected number of open statements: want 2, got %d", stmts)
	}
}

func TestPreparedExec(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewPrepared(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	_, err := db.ExecContext(ctx, "query 1")
	if err != nil {
		t.Error(err)
	}

	if stmts := driver.stmts.Load(); stmts != 1 {
		t.Errorf("unexpected number of open statements: want %d, got %d", 1, stmts)
	}

	_, err = db.ExecContext(ctx, "query 2")
	if err != nil {
		t.Error(err)
	}

	if stmts := driver.stmts.Load(); stmts != 2 {
		t.Errorf("unexpected number of open statements: want 2, got %d", stmts)
	}
}

func TestPreparedTx(t *testing.T) {
	ctx := context.Background()
	driver := &testDriver{
		t: t,
	}
	db := database.NewPrepared(sql.OpenDB(&testConnector{driver: driver}))
	defer db.Close()

	transaction := func() error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return (err)
		}
		defer tx.Rollback()

		_, err = tx.ExecContext(ctx, "query 1")
		if err != nil {
			return (err)
		}

		_, err = tx.ExecContext(ctx, "query 2")
		if err != nil {
			return (err)
		}
		return nil
	}

	for i := 0; i < 5; i++ {
		if err := transaction(); err != nil {
			t.Error(err)
		}
	}

	if stmts := driver.stmts.Load(); stmts != 4 {
		t.Errorf("unexpected number of open statements: want 2, got %d", stmts)
	}

	_, err := db.ExecContext(ctx, "query 2")
	if err != nil {
		t.Error(err)
	}

	if stmts := driver.stmts.Load(); stmts != 4 {
		t.Errorf("unexpected number of open statements: want 2, got %d", stmts)
	}
}
