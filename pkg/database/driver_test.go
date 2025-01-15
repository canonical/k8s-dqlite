package database_test

import (
	"context"
	"database/sql/driver"
	"io"
	"sync/atomic"
	"testing"
)

type testDriver struct {
	t       *testing.T
	conns   atomic.Int32
	queries atomic.Int32
	execs   atomic.Int32
	stmts   atomic.Int32
	trans   atomic.Int32
}

var _ driver.DriverContext = &testDriver{}

// Open implements driver.Driver.
func (td *testDriver) Open(name string) (driver.Conn, error) {
	td.conns.Add(1)
	return &testConn{
		driver: td,
		dbName: name,
	}, nil
}

func (td *testDriver) OpenConnector(name string) (driver.Connector, error) {
	return &testConnector{
		driver: td,
		dbName: name,
	}, nil
}

func (td *testDriver) Close() {
	if rows := td.queries.Load(); rows != 0 {
		td.t.Errorf("%d rows left open after close", rows)
	}
	if stmts := td.stmts.Load(); stmts != 0 {
		td.t.Errorf("%d statements left open after close", stmts)
	}
	if conns := td.conns.Load(); conns != 0 {
		td.t.Errorf("%d connections left open after close", conns)
	}
	if trans := td.trans.Load(); trans != 0 {
		td.t.Errorf("%d transactions left open after close", trans)
	}
}

type testConnector struct {
	driver *testDriver
	dbName string
}

func (tc *testConnector) Driver() driver.Driver { return tc.driver }

func (tc *testConnector) Connect(context.Context) (driver.Conn, error) {
	tc.driver.conns.Add(1)
	return &testConn{
		driver: tc.driver,
		dbName: tc.dbName,
	}, nil
}

type testConn struct {
	driver *testDriver
	dbName string
}

var _ driver.ExecerContext = &testConn{}
var _ driver.QueryerContext = &testConn{}

func (t *testConn) Begin() (driver.Tx, error) {
	t.driver.trans.Add(1)
	return &testTx{
		driver: t.driver,
	}, nil
}

func (t *testConn) Prepare(query string) (driver.Stmt, error) {
	t.driver.stmts.Add(1)
	return &testStmt{
		driver: t.driver,
		query:  query,
	}, nil
}

func (t *testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	t.driver.queries.Add(1)
	return &testRows{
		driver: t.driver,
	}, nil
}

func (t *testConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	t.driver.execs.Add(1)
	return testResult{}, nil
}

func (t *testConn) Close() error { return nil }

type testStmt struct {
	driver *testDriver
	query  string
}

func (t *testStmt) NumInput() int { return 0 }

func (t *testStmt) Exec(args []driver.Value) (driver.Result, error) {
	t.driver.execs.Add(1)
	return testResult{}, nil
}

func (t *testStmt) Query(args []driver.Value) (driver.Rows, error) {
	t.driver.queries.Add(1)
	return &testRows{
		driver: t.driver,
	}, nil
}

func (t *testStmt) Close() error { return nil }

type testRows struct {
	driver *testDriver
}

func (t *testRows) Columns() []string              { return nil }
func (t *testRows) Next(dest []driver.Value) error { return io.EOF }
func (t *testRows) Close() error                   { return nil }

type testResult struct{}

func (t testResult) LastInsertId() (int64, error) { return 0, nil }
func (t testResult) RowsAffected() (int64, error) { return 0, nil }

type testTx struct {
	driver *testDriver
}

func (t *testTx) Commit() error   { return nil }
func (t *testTx) Rollback() error { return nil }
