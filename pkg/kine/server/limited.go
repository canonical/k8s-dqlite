package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	backend Backend
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		ctx, span := tracer.Start(ctx, "limited.get")
		defer span.End()
		return l.get(ctx, r)
	}
	ctx, span := tracer.Start(ctx, "limited.list")
	defer span.End()
	return l.list(ctx, r)
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		ctx, span := tracer.Start(ctx, "limited.create")
		defer span.End()
		return l.create(ctx, put, txn)
	}
	if rev, key, ok := isDelete(txn); ok {
		ctx, span := tracer.Start(ctx, "limited.delete")
		defer span.End()
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return l.compact(ctx)
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
