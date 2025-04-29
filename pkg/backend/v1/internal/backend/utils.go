package backend

import (
	"bytes"
	"database/sql"

	"github.com/canonical/k8s-dqlite/pkg/limited"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func ScanAll[T any](rows *sql.Rows, scanOne func(*sql.Rows) (T, error)) ([]T, error) {
	var result []T
	defer rows.Close()

	for rows.Next() {
		item, err := scanOne(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

func scanKeyValue(rows *sql.Rows) (*limited.KeyValue, error) {
	kv := &limited.KeyValue{}
	err := rows.Scan(
		&kv.ModRevision,
		&kv.Key,
		&kv.CreateRevision,
		&kv.Lease,
		&kv.Value,
	)
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func scanEvent(rows *sql.Rows) (*limited.Event, error) {
	// Bundle in a single allocation
	row := &struct {
		event            limited.Event
		kv, prevKv       limited.KeyValue
		created, deleted bool
	}{}
	event := &row.event

	err := rows.Scan(
		&row.kv.ModRevision,
		&row.kv.Key,
		&row.created,
		&row.deleted,
		&row.kv.CreateRevision,
		&row.prevKv.ModRevision,
		&row.kv.Lease,
		&row.kv.Value,
		&row.prevKv.Value,
	)
	if err != nil {
		return nil, err
	}

	event.Kv = &row.kv
	if row.deleted {
		row.event.Type = mvccpb.DELETE
	}
	if row.created {
		event.Kv.CreateRevision = event.Kv.ModRevision
	} else {
		event.PrevKv = &row.prevKv
		event.PrevKv.Key = event.Kv.Key
		event.PrevKv.CreateRevision = event.Kv.CreateRevision
		event.PrevKv.Lease = event.Kv.Lease
	}
	return event, nil
}

func appendEvents(src, dst []*limited.Event, key, rangeEnd []byte) []*limited.Event {
	for _, event := range src {
		if bytes.Compare(key, event.Kv.Key) <= 0 && bytes.Compare(rangeEnd, event.Kv.Key) > 0 {
			dst = append(dst, event)
		}
	}
	return dst
}
