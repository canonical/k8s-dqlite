package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func isUpdate(txn *etcdserverpb.TxnRequest) (int64, string, []byte, int64, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil {
		return txn.Compare[0].GetModRevision(),
			string(txn.Compare[0].Key),
			txn.Success[0].GetRequestPut().Value,
			txn.Success[0].GetRequestPut().Lease,
			true
	}
	return 0, "", nil, 0, false
}

func (l *LimitedServer) update(ctx context.Context, rev int64, key string, value []byte, lease int64) (*etcdserverpb.TxnResponse, error) {
	var (
		kv  *KeyValue
		ok  bool
		err error
	)
	ctx, updateSpan := tracer.Start(ctx, "limited.update")
	defer updateSpan.End()
	updateSpan.SetAttributes(
		attribute.String("key", key),
		attribute.Int64("revision", rev),
		attribute.Int64("lease", lease),
	)

	if rev == 0 {
		ctx, span := tracer.Start(ctx, "backend.create")
		defer span.End()
		span.SetAttributes(
			attribute.String("key", key),
			attribute.Int64("lease", lease),
		)
		createCnt.Add(ctx, 1)

		rev, err = l.backend.Create(ctx, key, value, lease)
		ok = true
		span.SetAttributes(attribute.Bool("ok", ok))
		span.RecordError(err)
		span.End()

		ctx, span = tracer.Start(ctx, "backend.get")
		defer span.End()
		span.SetAttributes(
			attribute.String("key", key),
			attribute.Int64("revision", rev),
		)
		if err == ErrKeyExists {
			span.AddEvent("key exists")
			rev, kv, err = l.backend.Get(ctx, key, "", 1, rev)
			if err != nil {
				span.RecordError(err)
			}
			span.AddEvent(fmt.Sprintf("got revision %d", rev))
		} else {
			ok = true
		}
		span.End()
	} else {
		ctx, span := tracer.Start(ctx, "backend.update")
		defer span.End()
		span.SetAttributes(
			attribute.String("key", key),
			attribute.Int64("revision", rev),
			attribute.Int64("lease", lease),
		)
		updateCnt.Add(ctx, 1)

		rev, kv, ok, err = l.backend.Update(ctx, key, value, rev, lease)
		span.RecordError(err)
		span.SetAttributes(attribute.Bool("ok", ok))
	}
	if err != nil {
		return nil, err
	}

	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(rev),
		Succeeded: ok,
	}

	if ok {
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
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(rev),
						Kvs:    toKVs(kv),
					},
				},
			},
		}
	}

	return resp, nil
}
