package generic

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/sirupsen/logrus"
)

type genericWatcher struct {
	mu                   sync.Mutex
	ctx                  context.Context
	stop                 context.CancelFunc
	wg                   sync.WaitGroup
	dialect              *Generic
	listeners            map[int64]*genericListener
	addQueue, closeQueue []*genericListener
	nextListenerId       int64
	revision             int64
}

var _ server.Watcher = &genericWatcher{}

func (w *genericWatcher) Start(startCtx context.Context) error {
	revision, _, err := w.dialect.GetCompactRevision(startCtx)
	if err != nil {
		return err
	}
	w.revision = revision

	w.ctx, w.stop = context.WithCancel(context.Background())
	w.wg.Add(1)
	go func() {
		w.run(w.ctx)
		w.wg.Done()
	}()
	return nil
}

func (w *genericWatcher) connect(ctx context.Context) (conn *sql.Conn, err error) {
	conn, err = w.dialect.DB.Underlying().Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			if closeErr := conn.Close(); closeErr != nil {
				err = errors.Join(err, closeErr)
			}
		}
	}()

	if _, err := conn.ExecContext(ctx, `
		CREATE TEMP TABLE IF NOT EXISTS watcher(
			start TEXT NOT NULL,
			id INTEGER NOT NULL,
			end TEXT NOT NULL,
			PRIMARY KEY (start, id)
		) WITHOUT ROWID
	`); err != nil {
		return nil, err
	}

	// This shouldn't be necessary, but if for some
	// reason the connection was already used to
	// watch and it was returned to the pool instead
	// of being closed, then the watcher table needs
	// to be re-populated from scratch. In general,
	// it is expected that a connection is returned
	// only on watcher close, which will cleanup temp
	// tables. In all other cases the connection should
	// be faulted and should not be reused.
	if _, err := conn.ExecContext(ctx, `
		DELETE FROM watcher
	`); err != nil {
		return nil, err
	}

	// Re-add existing listeners.
	for _, l := range w.listeners {
		w.addQueue = append(w.addQueue, l)
	}

	return conn, nil
}

func (w *genericWatcher) run(ctx context.Context) {
	pollTicker := time.NewTicker(w.dialect.PollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := w.connect(ctx)
		if err != nil {
			logrus.WithError(err).Trace("polling error: cannot connect")
		}

		if err := w.poll(ctx, conn); err != nil {
			// Force the connection to be closed.
			conn.Raw(func(driverConn any) error { return driver.ErrBadConn })
			if closeErr := conn.Close(); closeErr != nil {
				logrus.WithError(closeErr).Trace("polling error: cannot close connection")
			}

			if err == context.Canceled || err == context.DeadlineExceeded {
				for _, l := range w.listeners {
					l.close(err)
				}
				w.listeners = make(map[int64]*genericListener)
			} else {
				logrus.WithError(err).Trace("polling error")
			}
		}
	}
}

const whatcherPollSize = 512

func (w *genericWatcher) poll(ctx context.Context, conn *sql.Conn) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	eventStmt, err := conn.PrepareContext(ctx, `
		WITH events AS MATERIALIZED (
			SELECT kine.id AS kine_id, watchers.id AS watcher_id
			FROM kine
				JOIN watchers ON kine.name >= watchers.start AND kine.name < watchers.end
			WHERE (kine.id / 1024) BETWEEN ($startRev / 1024) AND ($endRev / 1024)
				AND kine.id >= $startRev AND kine.id <= $endRev
		), groups AS (
			SELECT kine_id, group_concat(watcher_id, ",") AS watchers
			FROM events
			GROUP BY kine_id
		)
		SELECT kine.id as theid,
			kine.name,
			kine.created,
			kine.deleted,
			kine.create_revision,
			kine.prev_revision,
			kine.lease,
			kine.value,
			kine.old_value,
			watchers
		FROM groups CROSS JOIN kine
			ON kine.id = groups.kine_id`)
	if err != nil {
		return err
	}

	// Add new listeners
	for len(w.addQueue) > 0 {
		l := w.addQueue[0]
		_, err := conn.ExecContext(ctx, `
			INSERT INTO watcher(start, id, end)
			VALUES (?, ?, ?)
		`, l.rangeStart, l.id, l.rangeEnd)
		if err != nil {
			return err
		}

		w.listeners[l.id] = l
		w.addQueue = w.addQueue[1:]
	}
	w.addQueue = nil

	// Close pending listeners
	for len(w.closeQueue) > 0 {
		l := w.closeQueue[0]
		if _, err := conn.ExecContext(ctx, `
			DELETE FROM watcher
			WHERE start = ? AND id = ?
		`, l.rangeStart, l.id); err != nil {
			return err
		}

		delete(w.listeners, l.id)
		w.closeQueue = w.closeQueue[1:]
	}
	w.closeQueue = nil

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var compact, current sql.NullInt64
	row := tx.QueryRowContext(ctx, revisionIntervalSQL)
	if err := row.Scan(&compact, &current); err != nil {
		return err
	}

	if compact.Int64 > w.revision {
		// If a compaction was faster than the watchers
		// and removed some rows, then there is a problem
		// as there could be gaps in the event stream.
		// As such, all listeners should be closed.
		for _, l := range w.listeners {
			l.close(server.ErrCompacted)
		}
		w.listeners = make(map[int64]*genericListener)

		// Mark the current compaction version as
		// the next version that the watcher can
		// safely retrieve.
		w.revision = compact.Int64

		if _, err := tx.ExecContext(ctx, `
			DELETE FROM watcher
		`); err != nil {
			return err
		}

		return tx.Commit()
	}

	startRev := w.revision
	endRev := w.revision + whatcherPollSize
	if endRev > current.Int64 {
		endRev = current.Int64
	}

	rows, err := eventStmt.QueryContext(ctx,
		sql.Named("startRev", startRev),
		sql.Named("endRev", endRev),
	)
	if err != nil {
		return err
	}

	for rows.Next() {
		var watchers string
		event, err := scanEvent(rows, &watchers)
		if err != nil {
			return err
		}

		for _, watcher := range strings.Split(watchers, ",") {
			watcher, err := strconv.Atoi(watcher)
			if err != nil {
				return err
			}
			if listener := w.listeners[int64(watcher)]; listener != nil {
				listener.enqueueEvent(event)
			}
		}
	}

	for _, listener := range w.listeners {
		if listener.hasEvents() {
			listener.publishEvents()
		}
	}

	w.revision = endRev + 1
	return nil
}

func scanEvent(rows *sql.Rows, additionalColumns ...any) (*server.Event, error) {
	event := &server.Event{
		KV:     &server.KeyValue{},
		PrevKV: &server.KeyValue{},
	}

	args := []any{
		&event.KV.ModRevision,
		&event.KV.Key,
		&event.Create,
		&event.Delete,
		&event.KV.CreateRevision,
		&event.PrevKV.ModRevision,
		&event.KV.Lease,
		&event.KV.Value,
		&event.PrevKV.Value,
	}
	args = append(args, additionalColumns...)
	err := rows.Scan(args...)
	if err != nil {
		return nil, err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}
	return event, nil
}

func (w *genericWatcher) Watch(rangeStart, rangeEnd string, revision int64) server.Listener {
	w.mu.Lock()
	listener := &genericListener{
		id:            w.nextListenerId,
		revisionStart: revision,
		rangeStart:    rangeStart,
		rangeEnd:      rangeEnd,
		ch:            make(chan []*server.Event, 100),
	}
	w.nextListenerId++
	w.addQueue = append(w.addQueue, listener)
	streamRevision := w.revision
	w.mu.Unlock()

	if streamRevision >= revision {
		return listener
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		// Fetch first rows which are not directly streamed by the watch job
		// FIXME: this should fail if the compact revision has advanced. As such, this needs a small transaction.
		rows, err := w.dialect.DB.QueryContext(w.ctx, `
	SELECT id,
		name,
		created,
		deleted,
		create_revision,
		prev_revision,
		lease,
		value,
		old_value
	FROM kine
	WHERE name >= ? AND name < ?
		AND id >= ? AND id < ?
	ORDER BY id ASC
`, listener.rangeStart, listener.rangeEnd, listener.revisionStart, streamRevision)
		if err != nil {
			w.closeListener(listener, err)
			return
		}
		prevEvents := []*server.Event{}
		for rows.Next() {
			event, err := scanEvent(rows)
			if err != nil {
				w.closeListener(listener, err)
				return
			}
			prevEvents = append(prevEvents, event)
		}
		if err := rows.Err(); err != nil {
			w.closeListener(listener, err)
			return
		}

		w.mu.Lock()
		listener.eventBuffer = append(listener.eventBuffer)
		listener.ready = true
		w.mu.Unlock()
	}()

	return listener
}

func (w *genericWatcher) Listener(id int64) server.Listener {
	w.mu.Lock()
	defer w.mu.Unlock()
	listener := w.listeners[id]
	if listener == nil {
		for _, l := range w.addQueue {
			if l.id == id {
				listener = l
			}
		}
	}
	if listener != nil {
		for _, l := range w.addQueue {
			if l.id == id {
				listener = nil
			}
		}
	}
	return listener
}

func (w *genericWatcher) Stop() {
	w.mu.Lock()
	w.stop()
	w.mu.Unlock()
	w.wg.Wait()
}

func (w *genericWatcher) closeListener(l *genericListener, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closeQueue = append(w.closeQueue, l)
	l.close(nil)
}

type genericListener struct {
	id                   int64
	rangeStart, rangeEnd string
	revisionStart        int64
	ready                bool
	ch                   chan []*server.Event
	eventBuffer          []*server.Event
	w                    *genericWatcher
	err                  error
}

var _ server.Listener = &genericListener{}

func (l *genericListener) Id() int64                      { return l.id }
func (l *genericListener) RangeStart() string             { return l.rangeStart }
func (l *genericListener) RangeEnd() string               { return l.rangeEnd }
func (l *genericListener) Events() <-chan []*server.Event { return l.ch }
func (l *genericListener) Err() error                     { return l.err }

func (l *genericListener) close(err error) {
	if l.ch == nil {
		return
	}
	l.err = err
	close(l.ch)
	l.ch = nil
}

func (l *genericListener) Close() {
	l.w.closeListener(l, nil)
}

func (l *genericListener) enqueueEvent(event *server.Event) {
	if event.KV.ModRevision >= l.revisionStart {
		l.eventBuffer = append(l.eventBuffer, event)
	}
}

func (l *genericListener) hasEvents() bool {
	return len(l.eventBuffer) != 0
}

func (l *genericListener) publishEvents() {
	if !l.ready {
		select {
		case l.ch <- l.eventBuffer:
			l.eventBuffer = nil
		default:
			// Instead of blocking the event loop, it's better
			// to let the events accumulate a bit.
			// TODO: we should probably error if the queue becomes too big.
		}
	}
}
