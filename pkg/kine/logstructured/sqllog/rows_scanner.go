package sqllog

import (
	"database/sql"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
)

// rowsScanner is used as an iterim object to avoid
// allocation during rows iteration. It is effective
// mostly when returning a big amount of rows.
type rowsScanner struct {
	currentRev int64
	key        string
	create     bool
	delete     bool
	createRev  int64
	prevRev    int64
	lease      int64
	value      []byte
	prevValue  []byte
}

func (rs *rowsScanner) scan(rows *sql.Rows) (server.Event, error) {
	err := rows.Scan(
		&rs.currentRev,
		&rs.key,
		&rs.create,
		&rs.delete,
		&rs.createRev,
		&rs.prevRev,
		&rs.lease,
		&rs.value,
		&rs.prevValue,
	)
	if err != nil {
		return server.Event{}, err
	}

	if rs.create {
		return server.Event{
			Create: true,
			Delete: false,
			KV: &server.KeyValue{
				ModRevision:    rs.currentRev,
				Key:            rs.key,
				CreateRevision: rs.createRev,
				Lease:          rs.lease,
				Value:          rs.value,
			},
		}, nil
	} else {
		return server.Event{
			Create: false,
			Delete: rs.delete,
			KV: &server.KeyValue{
				ModRevision:    rs.currentRev,
				Key:            rs.key,
				CreateRevision: rs.createRev,
				Lease:          rs.lease,
				Value:          rs.value,
			},
			PrevKV: &server.KeyValue{
				ModRevision:    rs.prevRev,
				Key:            rs.key,
				CreateRevision: rs.createRev,
				Value:          rs.prevValue,
			},
		}, nil
	}
}
