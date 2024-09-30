package generic

import (
	"context"
	"database/sql"
	"sync"

	"github.com/canonical/k8s-dqlite/pkg/kine/prepared"
)

type BatchConn interface {
	ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error)
}

var _ BatchConn = &sql.DB{}
var _ BatchConn = &sql.Tx{}
var _ BatchConn = &sql.Conn{}

var _ BatchConn = &prepared.DB{}
var _ BatchConn = &prepared.Tx{}

type batchStatus int

const (
	batchNotStarted batchStatus = iota
	batchStarted
	batchRunning
)

type Batch struct {
	db     *prepared.DB
	mu     sync.Mutex
	cv     sync.Cond
	status batchStatus

	queue []*batchJob
	runId int64
}

func NewBatch(db *prepared.DB) *Batch {
	b := &Batch{
		db: db,
	}
	b.cv.L = &b.mu
	return b
}

func (b *Batch) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	runId := b.runId
	if b.status == batchRunning {
		// The current run is already taking place.
		runId++
	}

	job := &batchJob{
		ctx:   ctx,
		query: query,
		args:  args,
		runId: runId,
	}
	b.queue = append(b.queue, job)

	b.run()

	for job.runId >= b.runId {
		b.cv.Wait()
	}

	if job.err != nil {
		return nil, job.err
	}

	return job, nil
}

// run starts a batching job which will run until queue exaustion.
// run does not block other goroutine from enqueuing new jobs.
//
// It must be called while holding the mu lock.
func (b *Batch) run() {
	if b.status == batchNotStarted {
		b.status = batchStarted

		go func() {
			b.mu.Lock()
			defer b.mu.Unlock()

			b.status = batchRunning
			defer func() { b.status = batchNotStarted }()

			for len(b.queue) > 0 {
				queue := b.queue
				b.queue = nil

				b.mu.Unlock()
				b.execQueue(context.TODO(), queue)
				b.mu.Lock()

				b.runId++
				b.cv.Broadcast()
			}
		}()
	}
}

func (b *Batch) execQueue(ctx context.Context, queue []*batchJob) {
	// TODO limit batch duration
	// TODO limit batch size
	if len(queue) == 0 {
		return // This should never happen.
	}
	if len(queue) == 1 {
		// We don't need to address the error here as it will be
		// handled by the goroutine waiting for this result
		queue[0].exec(queue[0].ctx, b.db)
		return
	}

	transaction := func() error {
		// TODO: this should be BEGIN IMMEDIATE
		tx, err := b.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, q := range queue {
			// FIXME:
			// In the case of SQLITE_FULL SQLITE_IOERR SQLITE_BUSY SQLITE_NOMEM
			// we should explicitly rollback the whole transaction. However, it
			// is a bit unclear to me what to do next though as:
			//  - SQLITE_FULL, SQLITE_IOERR mean that we have problems with the disk
			//    so, even retrying the batch will not work. We might throttle the
			//    max batch size, hoping in a checkpoint?
			// - SQLITE_BUSY should never happen if we manage to get `IMMEDIATE`
			//   transactions in.
			// - SQLITE_NOMEM, again, we could throttle here?
			if err := q.exec(ctx, tx); err != nil {
				return err
			}
		}

		return tx.Commit()
	}

	if err := transaction(); err != nil {
		for _, q := range queue {
			q.err = err
		}
	}
}

type batchJob struct {
	ctx   context.Context
	query string
	args  []any

	runId        int64
	lastInsertId int64
	rowsAffected int64
	err          error
}

var _ sql.Result = &batchJob{}

func (bj *batchJob) exec(ctx context.Context, conn BatchConn) error {
	select {
	case <-bj.ctx.Done():
		bj.err = bj.ctx.Err()
		return bj.err
	default:
		// From this point on, the job is not interruptible anymore
		// as interrupting would mean that we would be forced to
		// ROLLBACK the whole transaction.
	}

	var result sql.Result
	result, bj.err = conn.ExecContext(ctx, bj.query, bj.args...)
	if bj.err != nil {
		return bj.err
	}

	bj.rowsAffected, bj.err = result.RowsAffected()
	if bj.err != nil {
		return bj.err
	}

	bj.lastInsertId, bj.err = result.LastInsertId()
	if bj.err != nil {
		return bj.err
	}

	return nil
}

// LastInsertId implements sql.Result.
func (bj *batchJob) LastInsertId() (int64, error) {
	return bj.lastInsertId, nil
}

// RowsAffected implements sql.Result.
func (bj *batchJob) RowsAffected() (int64, error) {
	return bj.rowsAffected, nil
}
