package limited

import (
	"context"
	"fmt"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	backend        Backend
	notifyInterval time.Duration
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Range", otelName))
	defer span.End()
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		resp, err := l.create(ctx, put)
		if err != nil {
			err = fmt.Errorf("create transaction failed: %w", err)
		}
		return resp, err
	}
	if rev, key, ok := isDelete(txn); ok {
		resp, err := l.delete(ctx, key, rev)
		if err != nil {
			err = fmt.Errorf("delete transaction failed for key %s: %w", string(key), err)
		}
		return resp, err
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		resp, err := l.update(ctx, rev, key, value, lease)
		if err != nil {
			err = fmt.Errorf("update transaction failed for key %s: %w", string(key), err)
		}
		return resp, err
	}
	if isCompact(txn) {
		resp, err := l.compact(ctx)
		if err != nil {
			err = fmt.Errorf("compact transaction failed: %w", err)
		}
		return resp, err
	}
	return nil, fmt.Errorf("unsupported transaction: %v", txn)
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
