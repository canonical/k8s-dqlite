package test

import (
	"context"
	"database/sql"
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
	client  *clientv3.Client
	backend server.Backend
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
	setup func(*sql.DB) error
}

// newKineServer spins up a new instance of kine. In case of an error, tb.Fatal is called.
func newKineServer(ctx context.Context, tb testing.TB, options *kineOptions) *kineServer {
	dir := tb.TempDir()

	err := instrument.StartSQLiteMonitoring()
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { instrument.StopSQLiteMonitoring() })

	var endpointConfig *endpoint.Config
	var db *sql.DB
	switch options.backendType {
	case endpoint.SQLiteBackend:
		endpointConfig, db = startSqlite(ctx, tb, dir)
	case endpoint.DQLiteBackend:
		endpointConfig, db = startDqlite(ctx, tb, dir)
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
		if err := options.setup(db); err != nil {
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
		client:  client,
		backend: backend,
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

var nextPort = 59090

func startDqlite(ctx context.Context, tb testing.TB, dir string) (*endpoint.Config, *sql.DB) {
	nextPort++

	app, err := app.New(dir, app.WithAddress(fmt.Sprintf("127.0.0.1:%d", nextPort)))
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
}

func (ks *kineServer) ResetMetrics() {
	instrument.ResetSQLiteMetrics()
}

func setupScenario(ctx context.Context, db *sql.DB, prefix string, numInsert, numUpdates, numDeletes int) error {
	t, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer t.Rollback()

	insertManyQuery := `
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
SELECT id + revision.base, ?||'/'||id, 1, 0, id + revision.base, 0, 0, 'value-'||id, NULL
FROM gen_id, revision`
	if _, err := t.ExecContext(ctx, insertManyQuery, numInsert, prefix); err != nil {
		return err
	}

	updateManyQuery := fmt.Sprintf(`
INSERT INTO kine(
	name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT kv.name, 0, 0, kv.create_revision, kv.id, 0, 'new-'||kv.value, kv.value
FROM kine AS kv
JOIN (
	SELECT MAX(mkv.id) as id
	FROM kine mkv
	WHERE  ?||'/' <= mkv.name AND mkv.name < ?||'0'
	GROUP BY mkv.name
) maxkv ON maxkv.id = kv.id
WHERE kv.deleted = 0
ORDER BY kv.name
LIMIT %d
		`, numUpdates)
	if _, err := t.ExecContext(ctx, updateManyQuery, prefix, prefix); err != nil {
		return err
	}

	deleteManyQuery := fmt.Sprintf(`
INSERT INTO kine(
	name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT kv.name, 0, 1, kv.create_revision, kv.id, 0, kv.value, kv.value
FROM kine AS kv
JOIN (
	SELECT MAX(mkv.id) as id
	FROM kine mkv
	WHERE  ?||'/' <= mkv.name AND mkv.name < ?||'0'
	GROUP BY mkv.name
) maxkv ON maxkv.id = kv.id
WHERE kv.deleted = 0
ORDER BY kv.name
LIMIT %d
		`, numDeletes)
	if _, err := t.ExecContext(ctx, deleteManyQuery, prefix, prefix); err != nil {
		return err
	}

	return t.Commit()
}
