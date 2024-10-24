package database

import (
	"context"
	"database/sql"
	"sync"
)

type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error)
}

type batchStatus int

const (
	batchNotStarted batchStatus = iota
	batchStarted
	batchRunning
)

type batchedDb struct {
	Interface

	mu     sync.Mutex
	cv     sync.Cond
	status batchStatus

	queue []*batchJob
	runId int64
}

func NewBatched(db Interface) Interface {
	b := &batchedDb{
		Interface: db,
	}
	b.cv.L = &b.mu
	return b
}

func (b *batchedDb) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
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

// run starts the batching job if it has not already been started
// which will run until queue exaustion. run does not block other
// goroutine from enqueuing new jobs.
//
// It must be called while holding the Batch's mu lock.
func (b *batchedDb) run() {
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

func (b *batchedDb) execQueue(ctx context.Context, queue []*batchJob) {
	if len(queue) == 0 {
		return // This should never happen.
	}
	if len(queue) == 1 {
		// We don't need to address the error here as it will be
		// handled by the goroutine waiting for this result
		queue[0].exec(queue[0].ctx, b.Interface)
		return
	}

	transaction := func() error {
		tx, err := b.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer tx.Rollback()

		for _, q := range queue {
			// In the case of SQLITE_FULL SQLITE_IOERR SQLITE_BUSY SQLITE_NOMEM
			// we should explicitly rollback the whole transaction. In all the other
			// cases, we could keep going with other queries. However, it is a bit
			// unclear to me what to do next though as:
			//  - SQLITE_FULL, SQLITE_IOERR mean that we have problems with the disk
			//    so, even retrying the batch will not work. We might throttle the
			//    max batch size, hoping in a checkpoint?
			// - SQLITE_BUSY should never happen if we manage to get `IMMEDIATE`
			//   transactions in. Otherwise it only affects the first statement.
			// - SQLITE_NOMEM, again, we could throttle here? Call a gc collection?
			// Given the points above, the code below always rolls back the whole
			// batch. It might seem inefficient, but it should almost never happen.
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

func (job *batchJob) exec(ctx context.Context, execer Execer) error {
	select {
	case <-job.ctx.Done():
		job.err = job.ctx.Err()
		return job.err
	default:
		// From this point on, the job is not interruptible anymore
		// as interrupting would mean that we would be forced to
		// ROLLBACK the whole transaction.
	}

	var result sql.Result
	result, job.err = execer.ExecContext(ctx, job.query, job.args...)
	if job.err != nil {
		return job.err
	}

	job.rowsAffected, job.err = result.RowsAffected()
	if job.err != nil {
		return job.err
	}

	job.lastInsertId, job.err = result.LastInsertId()
	if job.err != nil {
		return job.err
	}

	return nil
}

// LastInsertId implements sql.Result.
func (job *batchJob) LastInsertId() (int64, error) {
	return job.lastInsertId, nil
}

// RowsAffected implements sql.Result.
func (job *batchJob) RowsAffected() (int64, error) {
	return job.rowsAffected, nil
}
