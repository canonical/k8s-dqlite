package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func isUpdate(txn *etcdserverpb.TxnRequest) (int64, []byte, []byte, int64, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil {
		return txn.Compare[0].GetModRevision(),
			txn.Compare[0].Key,
			txn.Success[0].GetRequestPut().Value,
			txn.Success[0].GetRequestPut().Lease,
			true
	}
	return 0, nil, nil, 0, false
}

func (l *LimitedServer) update(ctx context.Context, rev int64, key, value []byte, lease int64) (_ *etcdserverpb.TxnResponse, err error) {
	updateCnt.Add(ctx, 1)

	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.update", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.Int64("lease", lease),
		attribute.Int64("revision", rev),
	)

	var succeeded bool
	if rev == 0 {
		rev, succeeded, err = l.backend.Create(ctx, key, value, lease)
	} else {
		rev, succeeded, err = l.backend.Update(ctx, key, value, rev, lease)
	}
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Bool("updated", succeeded), attribute.Int64("revision", rev))

	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(rev),
		Succeeded: succeeded,
	}

	if succeeded {
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: txnHeader(rev),
					},
				},
			},
		}
	} else {
		rev, kv, err := l.backend.List(ctx, key, nil, 1, rev)
		if err != nil {
			return nil, err
		}
		resp.Responses = []*etcdserverpb.ResponseOp{{
			Response: &etcdserverpb.ResponseOp_ResponseRange{
				ResponseRange: &etcdserverpb.RangeResponse{
					Header: txnHeader(rev),
					Kvs:    toKVs(kv...),
				},
			},
		}}
	}

	return resp, nil
}
