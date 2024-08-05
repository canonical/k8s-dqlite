package generic

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/prepared"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "generic"

var (
	otelTracer       trace.Tracer
	otelMeter        metric.Meter
	compactCnt       metric.Int64Counter
	getRevisionCnt   metric.Int64Counter
	deleteRevCnt     metric.Int64Counter
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
	getRevisionCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.get_revision", otelName), metric.WithDescription("Number of get revision requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	deleteRevCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.delete_revision", otelName), metric.WithDescription("Number of delete revision requests"))
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

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"

	revSQL = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv`

	listSQL = fmt.Sprintf(`
		SELECT %s
		FROM kine kv
		JOIN (
			SELECT MAX(mkv.id) as id
			FROM kine mkv
			WHERE
				mkv.name >= ? AND mkv.name < ?
				%%s
			GROUP BY mkv.name) maxkv
	    	ON maxkv.id = kv.id
		WHERE
			  (kv.deleted = 0 OR ?)
		ORDER BY kv.name ASC, kv.id ASC
	`, columns)

	revisionAfterSQL = fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT %s
			FROM kine AS kv
			JOIN (
				SELECT MAX(mkv.id) AS id
				FROM kine AS mkv
				WHERE mkv.name >= ? AND mkv.name < ?
					AND mkv.id <= ?
				GROUP BY mkv.name
			) AS maxkv
				ON maxkv.id = kv.id
			WHERE
				? OR kv.deleted = 0
		) AS lkv
		ORDER BY lkv.name ASC, lkv.theid ASC
	`, columns)

	revisionIntervalSQL = `
		SELECT (
			SELECT MAX(prev_revision)
			FROM kine
			WHERE name = 'compact_rev_key'
		) AS low, (
			SELECT MAX(id)
			FROM kine
		) AS high`
)

const maxRetries = 500

type Stripped string

func (s Stripped) String() string {
	str := strings.ReplaceAll(string(s), "\n", "")
	return regexp.MustCompile("[\t ]+").ReplaceAllString(str, " ")
}

type ErrRetry func(error) bool
type TranslateErr func(error) error
type ErrCode func(error) string

type Generic struct {
	sync.Mutex

	LockWrites            bool
	LastInsertID          bool
	DB                    *prepared.DB
	GetCurrentSQL         string
	GetRevisionSQL        string
	RevisionSQL           string
	ListRevisionStartSQL  string
	GetRevisionAfterSQL   string
	CountCurrentSQL       string
	CountRevisionSQL      string
	AfterSQLPrefix        string
	AfterSQL              string
	DeleteSQL             string
	CompactSQL            string
	UpdateCompactSQL      string
	InsertSQL             string
	FillSQL               string
	InsertLastInsertIDSQL string
	CreateSQL             string
	GetSizeSQL            string
	Retry                 ErrRetry
	TranslateErr          TranslateErr
	ErrCode               ErrCode

	AdmissionControlPolicy AdmissionControlPolicy

	// CompactInterval is interval between database compactions performed by kine.
	CompactInterval time.Duration
	// PollInterval is the event poll interval used by kine.
	PollInterval time.Duration
}

func configureConnectionPooling(db *sql.DB) {
	db.SetMaxIdleConns(5)
	db.SetMaxOpenConns(5)
	db.SetConnMaxLifetime(60 * time.Second)
}

func q(sql, param string, numbered bool) string {
	if param == "?" && !numbered {
		return sql
	}

	regex := regexp.MustCompile(`\?`)
	n := 0
	return regex.ReplaceAllStringFunc(sql, func(string) string {
		if numbered {
			n++
			return param + strconv.Itoa(n)
		}
		return param
	})
}

func openAndTest(driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	for i := 0; i < 3; i++ {
		if err := db.Ping(); err != nil {
			db.Close()
			return nil, err
		}
	}

	return db, nil
}

func Open(ctx context.Context, driverName, dataSourceName string, paramCharacter string, numbered bool) (*Generic, error) {
	var (
		db  *sql.DB
		err error
	)
	for i := 0; i < 300; i++ {
		db, err = openAndTest(driverName, dataSourceName)
		if err == nil {
			break
		}

		logrus.Errorf("failed to ping connection: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	configureConnectionPooling(db)

	return &Generic{
		DB: prepared.New(db),

		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			%s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND mkv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(revisionAfterSQL, paramCharacter, numbered),

		CountCurrentSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(*)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

		CountRevisionSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(c.theid)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "AND mkv.id <= ?")), paramCharacter, numbered),

		AfterSQLPrefix: q(fmt.Sprintf(`
			SELECT %s
			FROM kine AS kv
			WHERE
				kv.name >= ? AND kv.name < ?
				AND kv.id > ?
			ORDER BY kv.id ASC`, columns), paramCharacter, numbered),

		AfterSQL: q(fmt.Sprintf(`
			SELECT %s
				FROM kine AS kv
				WHERE kv.id > ?
				ORDER BY kv.id ASC
		`, columns), paramCharacter, numbered),

		DeleteSQL: q(`
			DELETE FROM kine
			WHERE id = ?`, paramCharacter, numbered),

		UpdateCompactSQL: q(`
			UPDATE kine
			SET prev_revision = max(prev_revision, ?)
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),

		CreateSQL: q(`
			INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			SELECT 
				? AS name,
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
				WHERE name = ?
			) maxkv
			WHERE maxkv.deleted = 1 OR id IS NULL`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
		AdmissionControlPolicy: &allowAllPolicy{},
	}, err
}

func (d *Generic) Close() error {
	return d.DB.Close()
}

func getPrefixRange(prefix string) (start, end string) {
	start = prefix
	if strings.HasSuffix(prefix, "/") {
		end = prefix[0:len(prefix)-1] + "0"
	} else {
		// we are using only readable characters
		end = prefix + "\x01"
	}

	return start, end
}

func (d *Generic) query(ctx context.Context, txName, query string, args ...interface{}) (rows *sql.Rows, err error) {
	done, err := d.AdmissionControlPolicy.Admit(ctx, txName)
	if err != nil {
		return nil, fmt.Errorf("denied: %w", err)
	}
	defer done()

	start := time.Now()
	retryCount := 0
	defer func() {
		if err != nil {
			err = fmt.Errorf("query (try: %d): %w", retryCount, err)
		}
		recordOpResult(txName, err, start)
	}()
	for ; retryCount < maxRetries; retryCount++ {
		if retryCount == 0 {
			logrus.Tracef("QUERY (try: %d) %v : %s", retryCount, args, Stripped(query))
		} else {
			logrus.Debugf("QUERY (try: %d) %v : %s", retryCount, args, Stripped(query))
		}
		rows, err = d.DB.QueryContext(ctx, query, args...)
		if err == nil {
			break
		}
		if d.Retry == nil || !d.Retry(err) {
			break
		}
	}

	recordTxResult(txName, err)
	return rows, err
}

func (d *Generic) execute(ctx context.Context, txName, query string, args ...interface{}) (result sql.Result, err error) {
	done, err := d.AdmissionControlPolicy.Admit(ctx, txName)
	if err != nil {
		return nil, fmt.Errorf("denied: %w", err)
	}
	defer done()

	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	start := time.Now()
	retryCount := 0
	defer func() {
		if err != nil {
			err = fmt.Errorf("exec (try: %d): %w", retryCount, err)
		}
		recordOpResult(txName, err, start)
	}()
	for ; retryCount < maxRetries; retryCount++ {
		if retryCount > 2 {
			logrus.Debugf("EXEC (try: %d) %v : %s", retryCount, args, Stripped(query))
		} else {
			logrus.Tracef("EXEC (try: %d) %v : %s", retryCount, args, Stripped(query))
		}
		result, err = d.DB.ExecContext(ctx, query, args...)
		if err == nil {
			break
		}
		if d.Retry == nil || !d.Retry(err) {
			break
		}
	}

	recordTxResult(txName, err)
	return result, err
}

func (d *Generic) CountCurrent(ctx context.Context, prefix string, startKey string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	start, end := getPrefixRange(prefix)
	if startKey != "" {
		start = startKey + "\x01"
	}
	rows, err := d.query(ctx, "count_current", d.CountCurrentSQL, start, end, false)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, 0, err
		}
		return 0, 0, sql.ErrNoRows
	}

	if err := rows.Scan(&rev, &id); err != nil {
		return 0, 0, err
	}
	return rev.Int64, id, nil
}

func (d *Generic) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	start, end := getPrefixRange(prefix)
	if startKey != "" {
		start = startKey + "\x01"
	}
	rows, err := d.query(ctx, "count_revision", d.CountRevisionSQL, start, end, revision, false)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, 0, err
		}
		return 0, 0, sql.ErrNoRows
	}

	if err := rows.Scan(&rev, &id); err != nil {
		return 0, 0, err
	}
	return rev.Int64, id, err
}

func (d *Generic) Create(ctx context.Context, key string, value []byte, ttl int64) (rev int64, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Create", otelName))

	defer func() {
		if err != nil {
			if d.TranslateErr != nil {
				err = d.TranslateErr(err)
			}
			span.RecordError(err)
		}
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", key),
		attribute.Int64("ttl", ttl),
	)

	result, err := d.execute(ctx, "create_sql", d.CreateSQL, key, ttl, value, key)
	if err != nil {
		logrus.WithError(err).Error("failed to create key")
		return 0, err
	}

	if insertCount, err := result.RowsAffected(); err != nil {
		return 0, err
	} else if insertCount == 0 {
		return 0, server.ErrKeyExists
	}
	return result.LastInsertId()
}

// Compact compacts the database up to the revision provided in the method's call.
// After the call, any request for a version older than the given revision will return
// a compacted error.
func (d *Generic) Compact(ctx context.Context, revision int64) (err error) {
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
		if err == nil || d.Retry == nil || !d.Retry(err) {
			break
		}
	}
	return err
}

func (d *Generic) tryCompact(ctx context.Context, start, end int64) (err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.tryCompact", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("start", start), attribute.Int64("end", end))

	tx, err := d.DB.BeginTx(ctx, nil)
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

	if _, err = tx.ExecContext(ctx, d.UpdateCompactSQL, end); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, int64, error) {
	getCompactRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.get_compact_revision", otelName))
	var compact, target sql.NullInt64
	start := time.Now()
	var err error
	defer func() {
		span.RecordError(err)
		recordOpResult("revision_interval_sql", err, start)
		recordTxResult("revision_interval_sql", err)
		span.End()
	}()

	done, err := d.AdmissionControlPolicy.Admit(ctx, "revision_interval_sql")
	if err != nil {
		return 0, 0, fmt.Errorf("denied: %w", err)
	}
	defer done()

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

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	var err error
	getRevisionCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.get_revision", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("revision", revision))

	result, err := d.query(ctx, "get_revision_sql", d.GetRevisionSQL, revision)
	return result, err
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	var err error
	deleteRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.delete_revision", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("revision", revision))

	_, err = d.execute(ctx, "delete_sql", d.DeleteSQL, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	start, end := getPrefixRange(prefix)
	// NOTE(neoaggelos): don't ignore startKey if set
	if startKey != "" {
		start = startKey + "\x01"
	}

	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "get_current_sql_limit", sql, start, end, includeDeleted, limit)
	}
	return d.query(ctx, "get_current_sql", sql, start, end, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	start, end := getPrefixRange(prefix)
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT ?", sql)
			return d.query(ctx, "list_revision_start_sql_limit", sql, start, end, revision, includeDeleted, limit)
		}
		return d.query(ctx, "list_revision_start_sql", sql, start, end, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "get_revision_after_sql_limit", sql, startKey+"\x01", end, revision, includeDeleted, limit)
	}
	return d.query(ctx, "get_revision_after_sql", sql, startKey+"\x01", end, revision, includeDeleted)
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	var err error

	currentRevCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.current_revision", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	done, err := d.AdmissionControlPolicy.Admit(ctx, "rev_sql")
	if err != nil {
		return 0, fmt.Errorf("denied: %w", err)
	}
	defer done()

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

func (d *Generic) AfterPrefix(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	start, end := getPrefixRange(prefix)
	sql := d.AfterSQLPrefix
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "after_sql_prefix_limit", sql, start, end, rev, limit)
	}
	return d.query(ctx, "after_sql_prefix", sql, start, end, rev)
}

func (d *Generic) After(ctx context.Context, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT ?", sql)
		return d.query(ctx, "after_sql_limit", sql, rev, limit)
	}
	return d.query(ctx, "after_sql", sql, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.execute(ctx, "fill_sql", d.FillSQL, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
	return err
}

func (d *Generic) IsFill(key string) bool {
	return strings.HasPrefix(key, "gap-")
}

func (d *Generic) Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (id int64, err error) {
	if d.TranslateErr != nil {
		defer func() {
			if err != nil {
				err = d.TranslateErr(err)
			}
		}()
	}

	cVal := 0
	dVal := 0
	if create {
		cVal = 1
	}
	if delete {
		dVal = 1
	}

	if d.LastInsertID {
		row, err := d.execute(ctx, "insert_last_insert_id_sql", d.InsertLastInsertIDSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	rows, err := d.query(ctx, "insert_sql", d.InsertSQL, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
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

	if err := rows.Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}
	rows, err := d.query(ctx, "get_size_sql", d.GetSizeSQL)
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

func (d *Generic) GetCompactInterval() time.Duration {
	if v := d.CompactInterval; v > 0 {
		return v
	}
	return 5 * time.Minute
}

func (d *Generic) GetPollInterval() time.Duration {
	if v := d.PollInterval; v > 0 {
		return v
	}
	return time.Second
}
