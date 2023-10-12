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

	"github.com/Rican7/retry/jitter"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	columns = "kv.id as theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value"

	revSQL = `
		SELECT MAX(rkv.id) AS id
		FROM kine AS rkv`

	listSQL = fmt.Sprintf(`
		SELECT %s
		FROM kine AS kv
			LEFT JOIN kine kv2
				ON kv.name = kv2.name
				AND kv.id < kv2.id
		WHERE kv2.name IS NULL
			AND kv.name >= ? AND kv.name < ?
			AND (? OR kv.deleted = 0)
			%%s
		ORDER BY kv.id ASC
	`, columns)

	// FIXME this query doesn't seem sound.
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
						AND mkv.id > (
							SELECT ikv.id
							FROM kine AS ikv
							WHERE
								ikv.name = ? AND
								ikv.id <= ?
							ORDER BY ikv.id DESC
							LIMIT 1
						)
					GROUP BY mkv.name
				) AS maxkv
					ON maxkv.id = kv.id
				WHERE
					? OR kv.deleted = 0
			) AS lkv
			ORDER BY lkv.theid ASC
		`, columns)

	revisionIntervalSQL = `
		SELECT (
			SELECT crkv.prev_revision
			FROM kine AS crkv
			WHERE crkv.name = 'compact_rev_key'
			ORDER BY prev_revision
			DESC LIMIT 1
		) AS low, (
			SELECT id
			FROM kine
			ORDER BY id
			DESC LIMIT 1
		) AS high`
)

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

	LockWrites                    bool
	LastInsertID                  bool
	DB                            *sql.DB
	GetCurrentSQL                 string
	GetRevisionSQL                string
	getRevisionSQLPrepared        *sql.Stmt
	RevisionSQL                   string
	ListRevisionStartSQL          string
	GetRevisionAfterSQL           string
	CountSQL                      string
	countSQLPrepared              *sql.Stmt
	AfterSQLPrefix                string
	afterSQLPrefixPrepared        *sql.Stmt
	AfterSQL                      string
	DeleteSQL                     string
	deleteSQLPrepared             *sql.Stmt
	UpdateCompactSQL              string
	updateCompactSQLPrepared      *sql.Stmt
	InsertSQL                     string
	insertSQLPrepared             *sql.Stmt
	FillSQL                       string
	fillSQLPrepared               *sql.Stmt
	InsertLastInsertIDSQL         string
	insertLastInsertIDSQLPrepared *sql.Stmt
	GetSizeSQL                    string
	getSizeSQLPrepared            *sql.Stmt
	Retry                         ErrRetry
	TranslateErr                  TranslateErr
	ErrCode                       ErrCode

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
		DB: db,

		GetRevisionSQL: q(fmt.Sprintf(`
			SELECT
			%s
			FROM kine kv
			WHERE kv.id = ?`, columns), paramCharacter, numbered),

		GetCurrentSQL:        q(fmt.Sprintf(listSQL, ""), paramCharacter, numbered),
		ListRevisionStartSQL: q(fmt.Sprintf(listSQL, "AND kv.id <= ?"), paramCharacter, numbered),
		GetRevisionAfterSQL:  q(revisionAfterSQL, paramCharacter, numbered),

		CountSQL: q(fmt.Sprintf(`
			SELECT (%s), COUNT(*)
			FROM (
				%s
			) c`, revSQL, fmt.Sprintf(listSQL, "")), paramCharacter, numbered),

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
			SET prev_revision = ?
			WHERE name = 'compact_rev_key'`, paramCharacter, numbered),

		InsertLastInsertIDSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),

		InsertSQL: q(`INSERT INTO kine(name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?) RETURNING id`, paramCharacter, numbered),

		FillSQL: q(`INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`, paramCharacter, numbered),
		AdmissionControlPolicy: &allowAllPolicy{},
	}, err
}

func (d *Generic) Prepare() error {
	var err error

	d.getRevisionSQLPrepared, err = d.DB.Prepare(d.GetRevisionSQL)
	if err != nil {
		return err
	}

	d.countSQLPrepared, err = d.DB.Prepare(d.CountSQL)
	if err != nil {
		return err
	}

	d.deleteSQLPrepared, err = d.DB.Prepare(d.DeleteSQL)
	if err != nil {
		return err
	}

	d.getSizeSQLPrepared, err = d.DB.Prepare(d.GetSizeSQL)
	if err != nil {
		return err
	}

	d.fillSQLPrepared, err = d.DB.Prepare(d.FillSQL)
	if err != nil {
		return err
	}

	if d.LastInsertID {
		d.insertLastInsertIDSQLPrepared, err = d.DB.Prepare(d.InsertLastInsertIDSQL)
		if err != nil {
			return err
		}
	} else {
		d.insertSQLPrepared, err = d.DB.Prepare(d.InsertSQL)
		if err != nil {
			return err
		}
	}

	d.updateCompactSQLPrepared, err = d.DB.Prepare(d.UpdateCompactSQL)
	if err != nil {
		return err
	}

	d.afterSQLPrefixPrepared, err = d.DB.Prepare(d.AfterSQLPrefix)
	if err != nil {
		return err
	}

	return nil
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

func (d *Generic) query(ctx context.Context, txName, sql string, args ...interface{}) (rows *sql.Rows, err error) {
	i := uint(0)
	start := time.Now()

	done, err := d.AdmissionControlPolicy.Admit(ctx, txName)
	if err != nil {
		return nil, fmt.Errorf("denied: %w", err)
	}

	defer func() {
		if err != nil {
			err = fmt.Errorf("query (try: %d): %w", i, err)
		}
		recordOpResult(txName, err, start)
	}()

	strippedSQL := Stripped(sql)
	for ; i < 500; i++ {
		if i > 2 {
			logrus.Debugf("QUERY (try: %d) %v : %s", i, args, strippedSQL)
		} else {
			logrus.Tracef("QUERY (try: %d) %v : %s", i, args, strippedSQL)
		}
		rows, err = d.DB.QueryContext(ctx, sql, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		done()
		recordTxResult(txName, err)
		return rows, err
	}
	done()
	return
}

func (d *Generic) queryPrepared(ctx context.Context, txName, sql string, prepared *sql.Stmt, args ...interface{}) (result *sql.Rows, err error) {
	logrus.Tracef("QUERY %v : %s", args, Stripped(sql))

	done, err := d.AdmissionControlPolicy.Admit(ctx, txName)
	if err != nil {
		return nil, fmt.Errorf("denied: %w", err)
	}

	start := time.Now()
	r, err := prepared.QueryContext(ctx, args...)
	done()

	recordOpResult(txName, err, start)
	recordTxResult(txName, err)
	return r, err
}

func (d *Generic) queryRow(ctx context.Context, txName, sql string, args ...interface{}) (result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, Stripped(sql))
	start := time.Now()
	r := d.DB.QueryRowContext(ctx, sql, args...)
	recordOpResult(txName, r.Err(), start)
	recordTxResult(txName, r.Err())
	return r
}

func (d *Generic) queryRowPrepared(ctx context.Context, txName, sql string, prepared *sql.Stmt, args ...interface{}) (result *sql.Row) {
	logrus.Tracef("QUERY ROW %v : %s", args, Stripped(sql))
	start := time.Now()
	r := prepared.QueryRowContext(ctx, args...)
	recordOpResult(txName, r.Err(), start)
	recordTxResult(txName, r.Err())
	return r
}

func (d *Generic) executePrepared(ctx context.Context, txName, sql string, prepared *sql.Stmt, args ...interface{}) (result sql.Result, err error) {
	i := uint(0)
	start := time.Now()
	defer func() {
		if err != nil {
			err = fmt.Errorf("exec (try: %d): %w", i, err)
		}
		recordOpResult(txName, err, start)
	}()

	done, err := d.AdmissionControlPolicy.Admit(ctx, txName)
	if err != nil {
		return nil, fmt.Errorf("denied: %w", err)
	}

	if d.LockWrites {
		d.Lock()
		defer d.Unlock()
	}

	strippedSQL := Stripped(sql)
	for ; i < 500; i++ {
		if i > 2 {
			logrus.Debugf("EXEC (try: %d) %v : %s", i, args, strippedSQL)
		} else {
			logrus.Tracef("EXEC (try: %d) %v : %s", i, args, strippedSQL)
		}
		result, err = prepared.ExecContext(ctx, args...)
		if err != nil && d.Retry != nil && d.Retry(err) {
			time.Sleep(jitter.Deviation(nil, 0.3)(2 * time.Millisecond))
			continue
		}
		done()
		recordTxResult(txName, err)
		return result, err
	}
	done()
	return
}

func (d *Generic) GetCompactRevision(ctx context.Context) (int64, int64, error) {
	var compact, target sql.NullInt64
	start := time.Now()
	var err error
	defer func() {
		if err == sql.ErrNoRows {
			err = nil
		}
		recordOpResult("revision_interval_sql", err, start)
		recordTxResult("revision_interval_sql", err)
	}()

	done, err := d.AdmissionControlPolicy.Admit(ctx, "revision_interval_sql")
	if err != nil {
		return 0, 0, fmt.Errorf("denied: %w", err)
	}

	row := d.DB.QueryRow(revisionIntervalSQL)
	done()
	err = row.Scan(&compact, &target)
	if err == sql.ErrNoRows {
		return 0, 0, nil
	}

	return compact.Int64, target.Int64, err
}

func (d *Generic) SetCompactRevision(ctx context.Context, revision int64) error {
	_, err := d.executePrepared(ctx, "update_compact_sql", d.UpdateCompactSQL, d.updateCompactSQLPrepared, revision)
	return err
}

func (d *Generic) GetRevision(ctx context.Context, revision int64) (*sql.Rows, error) {
	return d.queryPrepared(ctx, "get_revision_sql", d.GetRevisionSQL, d.getRevisionSQLPrepared, revision)
}

func (d *Generic) DeleteRevision(ctx context.Context, revision int64) error {
	_, err := d.executePrepared(ctx, "delete_sql", d.DeleteSQL, d.deleteSQLPrepared, revision)
	return err
}

func (d *Generic) ListCurrent(ctx context.Context, prefix string, limit int64, includeDeleted bool) (*sql.Rows, error) {
	sql := d.GetCurrentSQL
	start, end := getPrefixRange(prefix)
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}

	return d.query(ctx, "get_current_sql", sql, start, end, includeDeleted)
}

func (d *Generic) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error) {
	start, end := getPrefixRange(prefix)
	if startKey == "" {
		sql := d.ListRevisionStartSQL
		if limit > 0 {
			sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
		}
		return d.query(ctx, "list_revision_start_sql", sql, start, end, revision, includeDeleted)
	}

	sql := d.GetRevisionAfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, "get_revision_after_sql", sql, start, end, revision, startKey, revision, includeDeleted)
}

func (d *Generic) Count(ctx context.Context, prefix string) (int64, int64, error) {
	var (
		rev sql.NullInt64
		id  int64
	)

	start, end := getPrefixRange(prefix)

	row := d.queryRowPrepared(ctx, "count_sql", d.CountSQL, d.countSQLPrepared, start, end, false)
	err := row.Scan(&rev, &id)

	return rev.Int64, id, err
}

func (d *Generic) CurrentRevision(ctx context.Context) (int64, error) {
	var id int64
	var err error

	done, err := d.AdmissionControlPolicy.Admit(ctx, "rev_sql")
	if err != nil {
		return 0, fmt.Errorf("denied: %w", err)
	}

	row := d.queryRow(ctx, "rev_sql", revSQL)
	done()
	err = row.Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return id, err
}

func (d *Generic) AfterPrefix(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error) {
	start, end := getPrefixRange(prefix)
	sql := d.AfterSQLPrefix
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, "after_sql_prefix", sql, start, end, rev)
}

func (d *Generic) After(ctx context.Context, rev, limit int64) (*sql.Rows, error) {
	sql := d.AfterSQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	return d.query(ctx, "after_sql", sql, rev)
}

func (d *Generic) Fill(ctx context.Context, revision int64) error {
	_, err := d.executePrepared(ctx, "fill_sql", d.FillSQL, d.fillSQLPrepared, revision, fmt.Sprintf("gap-%d", revision), 0, 1, 0, 0, 0, nil, nil)
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
		row, err := d.executePrepared(ctx, "insert_last_insert_id_sql", d.InsertLastInsertIDSQL, d.insertLastInsertIDSQLPrepared, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
		if err != nil {
			return 0, err
		}
		return row.LastInsertId()
	}

	row := d.queryRowPrepared(ctx, "insert_sql", d.InsertSQL, d.insertSQLPrepared, key, cVal, dVal, createRevision, previousRevision, ttl, value, prevValue)
	err = row.Scan(&id)

	return id, err
}

func (d *Generic) GetSize(ctx context.Context) (int64, error) {
	if d.GetSizeSQL == "" {
		return 0, errors.New("driver does not support size reporting")
	}
	var size int64
	row := d.queryRowPrepared(ctx, "get_size_sql", d.GetSizeSQL, d.getSizeSQLPrepared)
	if err := row.Scan(&size); err != nil {
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
