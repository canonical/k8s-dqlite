package test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/url"
	"path"
	"testing"
	"time"

	dqlitedrv "github.com/canonical/go-dqlite/v3"
	"github.com/canonical/go-dqlite/v3/app"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/drivers/dqlite"
	"github.com/canonical/k8s-dqlite/pkg/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/instrument"
	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/canonical/k8s-dqlite/pkg/sqllog"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	logrus.SetOutput(io.Discard)
	logrus.SetLevel(logrus.FatalLevel)
}

const (
	SQLiteBackend = "sqlite"
	DQLiteBackend = "dqlite"
)

type k8sDqliteServer struct {
	client         *clientv3.Client
	backend        limited.Backend
	dqliteListener *instrument.Listener
}

type k8sDqliteConfig struct {
	// backendType is the type of the k8s-dqlite backend. It can be either
	// SQLiteBackend or DQLiteBackend.
	backendType string

	// setup is a function to setup the database before a test or
	// benchmark starts. It is called after the endpoint started,
	// so that migration and database schema setup is already done.
	setup func(context.Context, *sql.Tx) error
}

// newK8sDqliteServer spins up a new instance of k8s-dqlite. In case of an error, tb.Fatal is called.
func newK8sDqliteServer(ctx context.Context, tb testing.TB, config *k8sDqliteConfig) *k8sDqliteServer {
	dir := tb.TempDir()

	if err := instrument.StartSQLiteMonitoring(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { instrument.StopSQLiteMonitoring() })

	var driver sqllog.Driver
	var db *sql.DB
	var dqliteListener *instrument.Listener
	switch config.backendType {
	case SQLiteBackend:
		driver, db = startSqlite(ctx, tb, dir)
	case DQLiteBackend:
		dqliteListener = instrument.NewListener("unix", path.Join(dir, "dqlite.sock"))
		if err := dqliteListener.Listen(ctx); err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() {
			dqliteListener.Close()
			if err := dqliteListener.Err(); err != nil {
				tb.Error(err)
			}
		})
		driver, db = startDqlite(ctx, tb, dir, dqliteListener)
	default:
		tb.Fatalf("Testing %s backend not supported", config.backendType)
	}

	if config.setup != nil {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			tb.Fatal(err)
		}
		if err := config.setup(ctx, tx); err != nil {
			rollbackErr := tx.Rollback()
			tb.Fatal(errors.Join(err, rollbackErr))
		}
		if err := tx.Commit(); err != nil {
			tb.Fatal(err)
		}
	}

	backend := sqllog.New(&sqllog.SQLLogConfig{
		Driver:            driver,
		CompactInterval:   5 * time.Minute,
		PollInterval:      1 * time.Second,
		WatchQueryTimeout: 20 * time.Second,
	})
	tb.Cleanup(func() {
		if err := backend.Close(); err != nil {
			tb.Error("cannot close backend", err)
		}
	})
	if err := backend.Start(ctx); err != nil {
		tb.Fatal(err)
	}

	listenUrl := (&url.URL{
		Scheme: "unix",
		Path:   path.Join(dir, "kine.sock"),
	}).String()

	_, err := endpoint.Listen(ctx, &endpoint.EndpointConfig{
		ListenAddress: listenUrl,
		Server:        limited.New(backend, 5*time.Second),
	})
	if err != nil {
		tb.Fatal(err)
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{listenUrl},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		client.Close()
	})

	return &k8sDqliteServer{
		client:         client,
		backend:        backend,
		dqliteListener: dqliteListener,
	}
}

func startSqlite(ctx context.Context, tb testing.TB, dir string) (*sqlite.Driver, *sql.DB) {
	dbPath := path.Join(dir, "data.db")

	dbUri := url.URL{
		Path: dbPath,
		RawQuery: url.Values{
			"_journal":      []string{"WAL"},
			"_synchronous":  []string{"FULL"},
			"_foreign_keys": []string{"1"},
		}.Encode(),
	}

	db, err := sql.Open("sqlite3", dbUri.String())
	if err != nil {
		tb.Fatal(err)
	}

	driver, err := sqlite.NewDriver(ctx, &sqlite.DriverConfig{
		DB: database.NewBatched(database.NewPrepared(db)),
	})
	if err != nil {
		tb.Fatal(err)
	}

	return driver, db
}

func startDqlite(ctx context.Context, tb testing.TB, dir string, listener *instrument.Listener) (*dqlite.Driver, *sql.DB) {
	app, err := app.New(dir,
		app.WithAddress(listener.Address),
		app.WithExternalConn(listener.Connect, listener.AcceptedConns),
		app.WithSnapshotParams(dqlitedrv.SnapshotParams{
			Threshold: 512,
			Trailing:  4096,
			Strategy:  dqlitedrv.TrailingStrategyDynamic,
		}),
	)
	if err != nil {
		tb.Fatalf("failed to create dqlite app: %v", err)
	}
	if err := app.Ready(ctx); err != nil {
		tb.Fatalf("failed to initialize dqlite: %v", err)
	}
	tb.Cleanup(func() {
		app.Close()
	})

	db, err := app.Open(ctx, "k8s")
	if err != nil {
		tb.Fatal(err)
	}

	if err := db.PingContext(ctx); err != nil {
		tb.Fatal(err)
	}

	driver, err := dqlite.NewDriver(ctx, &dqlite.DriverConfig{
		DB:  database.NewBatched(database.NewPrepared(db)),
		App: app,
	})
	if err != nil {
		tb.Fatal(err)
	}

	return driver, db
}

func (ks *k8sDqliteServer) ReportMetrics(b *testing.B) {
	sqliteMetrics := instrument.FetchSQLiteMetrics()
	b.ReportMetric(float64(sqliteMetrics.PageCacheHits+sqliteMetrics.PageCacheMisses)/float64(b.N), "page-reads/op")
	b.ReportMetric(float64(sqliteMetrics.PageCacheMisses)/float64(b.N), "page-cache-misses/op")
	b.ReportMetric(float64(sqliteMetrics.PageCacheSpills)/float64(b.N), "page-cache-spills/op")
	b.ReportMetric(float64(sqliteMetrics.PageCacheWrites)/float64(b.N), "page-writes/op")
	b.ReportMetric(float64(sqliteMetrics.TransactionReadTime)/float64(time.Second)/float64(b.N), "sec-reading/op")
	b.ReportMetric(float64(sqliteMetrics.TransactionWriteTime)/float64(time.Second)/float64(b.N), "sec-writing/op")

	if ks.dqliteListener != nil {
		dqliteMetrics := ks.dqliteListener.Metrics()
		b.ReportMetric(float64(dqliteMetrics.BytesRead)/float64(b.N), "network-bytes-read/op")
		b.ReportMetric(float64(dqliteMetrics.BytesWritten)/float64(b.N), "network-bytes-written/op")
	}
}

func (ks *k8sDqliteServer) ResetMetrics() {
	instrument.ResetSQLiteMetrics()
	if ks.dqliteListener != nil {
		ks.dqliteListener.ResetMetrics()
	}
}

func insertMany(ctx context.Context, tx *sql.Tx, prefix string, valueSize, n int) (int64, error) {
	const insertManyQuery = `
WITH RECURSIVE gen_id AS(
	SELECT 1 AS id

	UNION ALL

	SELECT id + 1
	FROM gen_id
	WHERE id < ?
), revision AS(
	SELECT COALESCE(MAX(id), 0) AS base
	FROM kine
)
INSERT INTO kine(
	id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT id + revision.base, ?||'/'||id, 1, 0, id + revision.base, 0, 0, randomblob(?), NULL
FROM gen_id, revision`
	result, err := tx.ExecContext(ctx, insertManyQuery, n, prefix, valueSize)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func updateMany(ctx context.Context, tx *sql.Tx, prefix string, valueSize, n int) (int64, error) {
	const updateManyQuery = `
WITH maxkv AS (
	SELECT MAX(id) AS id
	FROM kine
	WHERE
		?||'/' <= name AND name < ?||'0'
	GROUP BY name
	HAVING deleted = 0
	ORDER BY name
)
INSERT INTO kine(
	name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT kv.name, 0, 0, kv.create_revision, kv.id, 0, randomblob(?), kv.value
FROM maxkv CROSS JOIN kine kv
	ON maxkv.id = kv.id
LIMIT ?`
	result, err := tx.ExecContext(ctx, updateManyQuery, valueSize, prefix, prefix, n)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func deleteMany(ctx context.Context, tx *sql.Tx, prefix string, n int) (int64, error) {
	const deleteManyQuery = `
WITH maxkv AS (
	SELECT MAX(id) AS id
	FROM kine
	WHERE
		?||'/' <= name AND name < ?||'0'
	GROUP BY name
	HAVING deleted = 0
	ORDER BY name
)
INSERT INTO kine(
	name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT kv.name, 0, 1, kv.create_revision, kv.id, 0, kv.value, kv.value
FROM maxkv CROSS JOIN kine kv
	ON maxkv.id = kv.id
LIMIT ?`
	result, err := tx.ExecContext(ctx, deleteManyQuery, prefix, prefix, n)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}
