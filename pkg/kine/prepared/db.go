package prepared

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
)

type DB struct {
	db      *sql.DB
	mu      sync.Mutex
	maxSize int
	store   map[string]*lruEntry
	lruList lruList
}

func New(db *sql.DB, maxSize int) *DB {
	return &DB{
		db:      db,
		maxSize: maxSize,
		store:   make(map[string]*lruEntry, maxSize),
	}
}

func (db *DB) Underlying() *sql.DB { return db.db }

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stms, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stms.ExecContext(ctx, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stms, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stms.QueryContext(ctx, args...)
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	errs := []error{}
	for len(db.store) != 0 {
		if err := db.drop(db.lruList.tail()); err != nil {
			errs = append(errs, err)
		}
	}

	if err := db.db.Close(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

func (db *DB) touch(entry *lruEntry) {
	db.lruList.remove(entry)
	db.lruList.add(entry)
}

func (db *DB) prepare(ctx context.Context, query string) (*stmt, error) {
	if stmt := db.get(query); stmt != nil {
		return stmt, nil
	}
	prepared, err := db.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	result := &stmt{prepared, atomic.Int32{}}
	db.put(query, result)
	return result.Ref(), nil
}

func (db *DB) get(key string) *stmt {
	db.mu.Lock()
	defer db.mu.Unlock()

	if entry, ok := db.store[key]; ok {
		db.touch(entry)
		return entry.stmt.Ref()
	}
	return nil
}

func (db *DB) put(query string, stmt *stmt) {
	if db.maxSize <= 0 {
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	entry, ok := db.store[query]
	if ok {
		db.touch(entry)
		entry.stmt.Close()
		entry.stmt = stmt.Ref()
		return
	}

	if len(db.store) >= db.maxSize {
		db.drop(db.lruList.tail())
	}

	entry = &lruEntry{
		query: query,
		stmt:  stmt.Ref(),
	}
	db.store[query] = entry
	db.lruList.add(entry)
}

func (db *DB) drop(entry *lruEntry) error {
	db.lruList.remove(entry)
	delete(db.store, entry.query)
	return entry.stmt.Close()
}

type stmt struct {
	*sql.Stmt
	count atomic.Int32
}

func (s *stmt) Ref() *stmt {
	s.count.Add(1)
	return s
}

func (s *stmt) Close() error {
	if s.count.Add(-1) <= 0 {
		return s.Stmt.Close()
	}
	return nil
}

type lruList struct {
	// lruList is implemented as a circular linked list
	head *lruEntry
}

type lruEntry struct {
	query string
	stmt  *stmt

	// Adjacent entries, unless at the tail of the list, the
	// next entry was added less recently tham the current.
	// Likewise prev was added more recently.
	next, prev *lruEntry
}

func (ll *lruList) remove(entry *lruEntry) {
	entry.prev.next = entry.next
	entry.next.prev = entry.prev
	if entry != ll.head {
		return
	}
	if entry.next == entry {
		ll.head = nil
	} else {
		ll.head = entry.next
	}
}

func (ll *lruList) add(entry *lruEntry) {
	if ll.head != nil {
		entry.prev = ll.head.prev
		entry.next = ll.head
		ll.head.prev.next = entry
		ll.head.prev = entry
	} else {
		entry.prev = entry
		entry.next = entry
	}
	ll.head = entry
}

func (ll *lruList) tail() *lruEntry {
	if ll.head == nil {
		return nil
	}
	return ll.head.prev
}
