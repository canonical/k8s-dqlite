package test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/k8s-dqlite/pkg/instrument"
	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func init() {
	logrus.SetOutput(io.Discard)
	logrus.SetLevel(logrus.FatalLevel)
}

type kineServer struct {
	client         *clientv3.Client
	backend        server.Backend
	dqliteListener *instrument.Listener
}

type kineOptions struct {
	// backendType is the type of the kine backend. It can be either
	// endpoint.SQLiteBackend or endpoint.DQLiteBackend.
	backendType string

	// endpointParameters can be used to configure kine parameters
	// like admission-control-**.
	endpointParameters []string

	// setup is a function to setup the database before a test or
	// benchmark starts. It is called after the endpoint started,
	// so that migration and database schema setup is already done.
	setup func(context.Context, *sql.Tx) error
}

// newKineServer spins up a new instance of kine. In case of an error, tb.Fatal is called.
func newKineServer(ctx context.Context, tb testing.TB, options *kineOptions) *kineServer {
	dir := tb.TempDir()

	if err := instrument.StartSQLiteMonitoring(); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { instrument.StopSQLiteMonitoring() })

	var endpointConfig *endpoint.Config
	var db *sql.DB
	var dqliteListener *instrument.Listener
	switch options.backendType {
	case endpoint.SQLiteBackend:
		endpointConfig, db = startSqlite(ctx, tb, dir)
	case endpoint.DQLiteBackend:
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
		endpointConfig, db = startDqlite(ctx, tb, dir, dqliteListener)
	default:
		tb.Fatalf("Testing %s backend not supported", options.backendType)
	}
	defer db.Close()

	if !strings.Contains(endpointConfig.Endpoint, "?") {
		endpointConfig.Endpoint += "?"
	}
	for _, param := range options.endpointParameters {
		endpointConfig.Endpoint = fmt.Sprintf("%s&%s", endpointConfig.Endpoint, param)
	}
	config, backend, err := endpoint.ListenAndReturnBackend(ctx, *endpointConfig)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		backend.Wait()
	})

	if options.setup != nil {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			tb.Fatal(err)
		}
		if err := options.setup(ctx, tx); err != nil {
			rollbackErr := tx.Rollback()
			tb.Fatal(errors.Join(err, rollbackErr))
		}
		if err := tx.Commit(); err != nil {
			tb.Fatal(err)
		}
	}

	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		tb.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointConfig.Listener},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return &kineServer{
		client:         client,
		backend:        backend,
		dqliteListener: dqliteListener,
	}
}

func startSqlite(_ context.Context, tb testing.TB, dir string) (*endpoint.Config, *sql.DB) {
	dbPath := path.Join(dir, "data.db")

	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		tb.Fatal(err)
	}

	return &endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/kine.sock", dir),
		Endpoint: fmt.Sprintf("sqlite://%s", dbPath),
	}, db
}

func startDqlite(ctx context.Context, tb testing.TB, dir string, listener *instrument.Listener) (*endpoint.Config, *sql.DB) {
	app, err := app.New(dir,
		app.WithAddress(listener.Address),
		app.WithExternalConn(listener.Connect, listener.AcceptedConns),
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

	return &endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/kine.sock", dir),
		Endpoint: fmt.Sprintf("dqlite://k8s?driver-name=%s", app.Driver()),
	}, db
}

func (ks *kineServer) ReportMetrics(b *testing.B) {
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

func (ks *kineServer) ResetMetrics() {
	instrument.ResetSQLiteMetrics()
	if ks.dqliteListener != nil {
		ks.dqliteListener.ResetMetrics()
	}
}

func insertMany(ctx context.Context, tx *sql.Tx, prefix string, valueSize, n int) error {
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
	_, err := tx.ExecContext(ctx, insertManyQuery, n, prefix, valueSize)
	return err
}

func updateMany(ctx context.Context, tx *sql.Tx, prefix string, valueSize, n int) error {
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
	_, err := tx.ExecContext(ctx, updateManyQuery, valueSize, prefix, prefix, n)
	return err
}

func deleteMany(ctx context.Context, tx *sql.Tx, prefix string, n int) error {
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
	_, err := tx.ExecContext(ctx, deleteManyQuery, prefix, prefix, n)
	return err
}
