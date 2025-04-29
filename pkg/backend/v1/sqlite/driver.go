package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	metrics "github.com/canonical/k8s-dqlite/pkg/backend/metrics"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	otelName = "sqlite"
)

var (
	otelTracer       trace.Tracer
	otelMeter        metric.Meter
	compactCnt       metric.Int64Counter
	compactBatchCnt  metric.Int64Counter
	deleteRevCnt     metric.Int64Counter
	deleteCnt        metric.Int64Counter
	createCnt        metric.Int64Counter
	updateCnt        metric.Int64Counter
	fillCnt          metric.Int64Counter
	currentRevCnt    metric.Int64Counter
	getCompactRevCnt metric.Int64Counter
)

func init() {
	var err error
	otelTracer = otel.Tracer(otelName)
	otelMeter = otel.Meter(otelName)
	compactCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.compact", otelName), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	compactBatchCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.compact_batch", otelName), metric.WithDescription("Number of compact batch requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	deleteRevCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.delete_rev", otelName), metric.WithDescription("Number of delete revision requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	createCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.create", otelName), metric.WithDescription("Number of create requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	updateCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.update", otelName), metric.WithDescription("Number of update requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	deleteCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.delete", otelName), metric.WithDescription("Number of delete requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	fillCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.fill", otelName), metric.WithDescription("Number of fill requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	currentRevCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.current_revision", otelName), metric.WithDescription("Current revision"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	getCompactRevCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.get_compact_revision", otelName), metric.WithDescription("Get compact revision"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
}

// Certain parameters such as keys or range limits are represented as byte slices,
// for which reason we need casts: CAST(? AS TEXT). Note that this should be a
// no-op for sqlite.
var (
	revSQL = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv`

	listSQL = `
		SELECT kv.id,
			name,
			CASE WHEN kv.created THEN kv.id ELSE kv.create_revision END AS create_revision,
			lease,
			value
		FROM kine AS kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine AS mkv
			WHERE
				mkv.name >= CAST(? AS TEXT) AND mkv.name < CAST(? AS TEXT)
				AND mkv.id <= ?
			GROUP BY mkv.name
		) AS maxkv
	    	ON maxkv.id = kv.id
		WHERE kv.deleted = 0
		ORDER BY kv.name ASC, kv.id ASC`

	revisionIntervalSQL = `
		SELECT (
			SELECT MAX(prev_revision)
			FROM kine
			WHERE name = 'compact_rev_key'
		) AS low, (
			SELECT MAX(id)
			FROM kine
		) AS high`

	countRevisionSQL = `
		SELECT COUNT(*)
		FROM kine AS kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine AS mkv
			WHERE
				mkv.name >= CAST(? AS TEXT) AND mkv.name < CAST(? AS TEXT)
				AND mkv.id <= ?
			GROUP BY mkv.name
		) AS maxkv
	    	ON maxkv.id = kv.id
		WHERE kv.deleted = 0`

	afterSQLPrefix = `
		SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
		FROM kine
		WHERE name >= CAST(? AS TEXT) AND name < CAST(? AS TEXT)
			AND ? < id AND id <= ?
		ORDER BY id ASC`

	afterSQL = `
		SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
		FROM kine
		WHERE id > ?
		ORDER BY id ASC`

	ttlSQL = `
		SELECT kv.id,
			name,
			lease
		FROM kine AS kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine AS mkv
			WHERE mkv.id <= ?
			GROUP BY mkv.name
		) AS maxkv
	    	ON maxkv.id = kv.id
		WHERE kv.deleted = 0 AND kv.lease != 0`

	deleteRevSQL = `
		DELETE FROM kine
		WHERE id = ?`

	updateCompactSQL = `
		UPDATE kine
		SET prev_revision = max(prev_revision, ?)
		WHERE name = 'compact_rev_key'`

	deleteSQL = `
		INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		SELECT
			name,
			0 AS created,
			1 AS deleted,
			CASE
				WHEN kine.created THEN id
				ELSE create_revision
			END AS create_revision,
			id AS prev_revision,
			lease,
			NULL AS value,
			value AS old_value
		FROM kine WHERE id = (SELECT MAX(id) FROM kine WHERE name = CAST(? AS TEXT))
			AND deleted = 0
			AND id = ?`

	createSQL = `
		INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		SELECT
			CAST(? AS TEXT) AS name,
			1 AS created,
			0 AS deleted,
			0 AS create_revision,
			COALESCE(id, 0) AS prev_revision,
			? AS lease,
			? AS value,
			NULL AS old_value
		FROM (
			SELECT MAX(id) AS id, deleted
			FROM kine
			WHERE name = CAST(? AS TEXT)
		) maxkv
		WHERE maxkv.deleted = 1 OR id IS NULL`

	updateSQL = `
		INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
		SELECT
			CAST(? AS TEXT) AS name,
			0 AS created,
			0 AS deleted,
			CASE
				WHEN kine.created THEN id
				ELSE create_revision
			END AS create_revision,
			id AS prev_revision,
			? AS lease,
			? AS value,
			value AS old_value
		FROM kine WHERE id = (SELECT MAX(id) FROM kine WHERE name = CAST(? AS TEXT))
			AND deleted = 0
			AND id = ?`

	getSizeSQL = `
		SELECT (page_count - freelist_count) * page_size
		FROM pragma_page_count(), pragma_page_size(), pragma_freelist_count()`
)

const maxRetries = 500

type Stripped string

func (s Stripped) String() string {
	builder := &strings.Builder{}
	whitespace := true
	for _, r := range s {
		if unicode.IsSpace(r) {
			if whitespace {
				continue
			}
			whitespace = true
			builder.WriteRune(' ')
		} else {
			whitespace = false
			builder.WriteRune(r)
		}
	}
	return builder.String()
}

type Driver struct {
	config *DriverConfig
}

type DriverConfig struct {
	DB    database.Interface
	Retry func(error) bool
}

func NewDriver(ctx context.Context, config *DriverConfig) (*Driver, error) {
	const retryAttempts = 300

	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	if config.DB == nil {
		return nil, errors.New("db cannot be nil")
	}
	if config.Retry == nil {
		config.Retry = func(err error) bool {
			if err, ok := err.(sqlite3.Error); ok {
				return err.Code == sqlite3.ErrBusy
			}
			return false
		}
	}

	for i := 0; i < retryAttempts; i++ {
		err := setup(ctx, config.DB)
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return &Driver{
		config: config,
	}, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(ctx context.Context, db database.Interface) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Optimistically ask for the user_version without starting a transaction
	var currentSchemaVersion SchemaVersion

	row := conn.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&currentSchemaVersion); err != nil {
		return err
	}

	if err := currentSchemaVersion.CompatibleWith(databaseSchemaVersion); err != nil {
		return err
	}
	if currentSchemaVersion >= databaseSchemaVersion {
		return nil
	}

	txn, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := migrate(ctx, txn); err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}

	return txn.Commit()
}

// migrate tries to migrate from a version of the database
// to the target one.
func migrate(ctx context.Context, txn *sql.Tx) error {
	var currentSchemaVersion SchemaVersion

	row := txn.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&currentSchemaVersion); err != nil {
		return err
	}

	if err := currentSchemaVersion.CompatibleWith(databaseSchemaVersion); err != nil {
		return err
	}
	if currentSchemaVersion >= databaseSchemaVersion {
		return nil
	}

	switch currentSchemaVersion {
	case NewSchemaVersion(0, 0):
		if err := applySchemaV0_1(ctx, txn); err != nil {
			return err
		}
	default:
		return nil
	}

	setUserVersionSQL := fmt.Sprintf(`PRAGMA user_version = %d`, databaseSchemaVersion)
	if _, err := txn.ExecContext(ctx, setUserVersionSQL); err != nil {
		return err
	}

	return nil
}

func (d *Driver) query(ctx context.Context, txName, query string, args ...interface{}) (rows *sql.Rows, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.query", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("tx_name", txName),
	)

	start := time.Now()
	retryCount := 0
	defer func() {
		if err != nil {
			err = fmt.Errorf("query (try: %d): %w", retryCount, err)
		}
		metrics.RecordOpResult(txName, err, start)
	}()
	for ; retryCount < maxRetries; retryCount++ {
		if retryCount == 0 {
			logrus.Tracef("QUERY (try: %d) %v : %s", retryCount, args, Stripped(query))
		} else {
			logrus.Debugf("QUERY (try: %d) %v : %s", retryCount, args, Stripped(query))
		}
		rows, err = d.config.DB.QueryContext(ctx, query, args...)
		if err == nil {
			break
		}
		if !d.config.Retry(err) {
			break
		}
	}

	metrics.RecordTxResult(txName, err)
	return rows, err
}

func (d *Driver) execute(ctx context.Context, txName, query string, args ...interface{}) (result sql.Result, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.execute", otelName))
	defer func() {
		span.RecordError(err)
		span.End()

	}()
	span.SetAttributes(
		attribute.String("tx_name", txName),
	)

	start := time.Now()
	retryCount := 0
	defer func() {
		if err != nil {
			err = fmt.Errorf("exec (try: %d): %w", retryCount, err)
		}
		metrics.RecordOpResult(txName, err, start)
	}()
	for ; retryCount < maxRetries; retryCount++ {
		if retryCount > 2 {
			logrus.Debugf("EXEC (try: %d) %v : %s", retryCount, args, Stripped(query))
		} else {
			logrus.Tracef("EXEC (try: %d) %v : %s", retryCount, args, Stripped(query))
		}
		result, err = d.config.DB.ExecContext(ctx, query, args...)
		if err == nil {
			break
		}
		if !d.config.Retry(err) {
			break
		}
	}

	metrics.RecordTxResult(txName, err)
	return result, err
}

func (d *Driver) Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, error) {
	if len(rangeEnd) == 0 {
		rangeEnd = append(key, 0)
	}
	rows, err := d.query(ctx, "count_revision", countRevisionSQL, key, rangeEnd, revision)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, sql.ErrNoRows
	}

	var id int64
	if err := rows.Scan(&id); err != nil {
		return 0, err
	}
	return id, err
}

func (d *Driver) Create(ctx context.Context, key, value []byte, ttl int64) (rev int64, succeeded bool, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Create", otelName))

	defer func() {
		span.RecordError(err)
		span.SetAttributes(attribute.Int64("revision", rev))
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.Int64("ttl", ttl),
	)
	createCnt.Add(ctx, 1)

	result, err := d.execute(ctx, "create_sql", createSQL, key, ttl, value, key)
	if err != nil {
		logrus.WithError(err).Error("failed to create key")
		return 0, false, err
	}
	if insertCount, err := result.RowsAffected(); err != nil {
		return 0, false, err
	} else if insertCount == 0 {
		return 0, false, nil
	}
	rev, err = result.LastInsertId()
	return rev, true, err
}

func (d *Driver) Update(ctx context.Context, key, value []byte, preRev, ttl int64) (rev int64, updated bool, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Update", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	updateCnt.Add(ctx, 1)
	result, err := d.execute(ctx, "update_sql", updateSQL, key, ttl, value, key, preRev)
	if err != nil {
		logrus.WithError(err).Error("failed to update key")
		return 0, false, err
	}
	if insertCount, err := result.RowsAffected(); err != nil {
		return 0, false, err
	} else if insertCount == 0 {
		return 0, false, nil
	}
	rev, err = result.LastInsertId()
	return rev, true, err
}

func (d *Driver) Delete(ctx context.Context, key []byte, revision int64) (rev int64, deleted bool, err error) {
	deleteCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Delete", otelName))
	defer func() {
		span.RecordError(err)
		span.SetAttributes(attribute.Int64("revision", rev))
		span.SetAttributes(attribute.Bool("deleted", deleted))
		span.End()
	}()
	span.SetAttributes(attribute.String("key", string(key)))

	result, err := d.execute(ctx, "delete_sql", deleteSQL, key, revision)
	if err != nil {
		logrus.WithError(err).Error("failed to delete key")
		return 0, false, err
	}
	if insertCount, err := result.RowsAffected(); err != nil {
		return 0, false, err
	} else if insertCount == 0 {
		return 0, false, nil
	}

	rev, err = result.LastInsertId()
	return rev, true, err
}

// Compact compacts the database up to the revision provided in the method's call.
// After the call, any request for a version older than the given revision will return
// a compacted error.
func (d *Driver) Compact(ctx context.Context, revision int64) (err error) {
	compactCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Compact", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	compactStart, currentRevision, err := d.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	span.SetAttributes(
		attribute.Int64("compact_start", compactStart),
		attribute.Int64("current_revision", currentRevision), attribute.Int64("revision", revision),
	)
	if compactStart >= revision {
		return nil // Nothing to compact.
	}
	if revision > currentRevision {
		revision = currentRevision
	}

	for retryCount := 0; retryCount < maxRetries; retryCount++ {
		err = d.tryCompact(ctx, compactStart, revision)
		if err == nil || !d.config.Retry(err) {
			break
		}
	}
	return err
}

func (d *Driver) tryCompact(ctx context.Context, start, end int64) (err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.tryCompact", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("start", start), attribute.Int64("end", end))
	compactBatchCnt.Add(ctx, 1)

	tx, err := d.config.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			logrus.WithError(err).Trace("can't rollback compaction")
		}
	}()

	// This query adds `created = 0` as a condition
	// to mitigate a bug in vXXX where newly created
	// keys had `prev_revision = max(id)` instead of 0.
	// Even with the bug fixed, we still need to check
	// for that in older rows and if older peers are
	// still running. Given that we are not yet using
	// an index, it won't change much in performance
	// however, it should be included in a covering
	// index if ever necessary.
	if _, err = tx.ExecContext(ctx, `
		DELETE FROM kine
		WHERE id IN (
			SELECT prev_revision
			FROM kine
			WHERE name != 'compact_rev_key'
				AND created = 0
				AND prev_revision != 0
				AND ? < id AND id <= ?
		)
	`, start, end); err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, `
		DELETE FROM kine
		WHERE deleted = 1
			AND ? < id AND id <= ?
	`, start, end); err != nil {
		return err
	}

	if _, err = tx.ExecContext(ctx, updateCompactSQL, end); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *Driver) GetCompactRevision(ctx context.Context) (int64, int64, error) {
	getCompactRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.get_compact_revision", otelName))
	var compact, target sql.NullInt64
	start := time.Now()
	var err error
	defer func() {
		span.RecordError(err)
		metrics.RecordOpResult("revision_interval_sql", err, start)
		metrics.RecordTxResult("revision_interval_sql", err)
		span.End()
	}()

	rows, err := d.query(ctx, "revision_interval_sql", revisionIntervalSQL)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, 0, err
		}
		return 0, 0, fmt.Errorf("cannot get compact revision: aggregate query returned no rows")
	}

	if err := rows.Scan(&compact, &target); err != nil {
		return 0, 0, err
	}
	span.SetAttributes(attribute.Int64("compact", compact.Int64), attribute.Int64("target", target.Int64))
	return compact.Int64, target.Int64, err
}

func (d *Driver) DeleteRevision(ctx context.Context, revision int64) error {
	var err error
	deleteRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.delete_revision", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("revision", revision))

	_, err = d.execute(ctx, "delete_rev_sql", deleteRevSQL, revision)
	return err
}

func (d *Driver) List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (*sql.Rows, error) {
	if len(rangeEnd) == 0 {
		rangeEnd = append(key, 0)
	}
	sql := listSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "list_revision_start_sql_limit", sql, key, rangeEnd, revision, limit)
	}
	return d.query(ctx, "list_revision_start_sql", sql, key, rangeEnd, revision)
}

func (d *Driver) ListTTL(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.query(ctx, "ttl_sql", ttlSQL, revision)
}

func (d *Driver) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	var err error

	currentRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.current_revision", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	rows, err := d.query(ctx, "rev_sql", revSQL)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("can't get current revision: aggregate query returned empty set")
	}

	if err := rows.Scan(&id); err != nil {
		return 0, err
	}
	span.SetAttributes(attribute.Int64("id", id))
	return id, nil
}

func (d *Driver) AfterPrefix(ctx context.Context, key, rangeEnd []byte, startRevision, endRevision int64) (*sql.Rows, error) {
	if len(rangeEnd) == 0 {
		rangeEnd = append(key, 0)
	}
	return d.query(ctx, "after_sql_prefix", afterSQLPrefix, key, rangeEnd, startRevision, endRevision)
}

func (d *Driver) After(ctx context.Context, rev, limit int64) (*sql.Rows, error) {
	sql := afterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "after_sql_limit", sql, rev, limit)
	}
	return d.query(ctx, "after_sql", sql, rev)
}

func (d *Driver) GetSize(ctx context.Context) (int64, error) {
	rows, err := d.query(ctx, "get_size_sql", getSizeSQL)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, sql.ErrNoRows
	}

	var size int64
	if err := rows.Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}

func (d *Driver) Close() error {
	return d.config.DB.Close()
}
