package server

import (
	"context"

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

	if rev == 0 {
		ctx, span := tracer.Start(ctx, "backend.create")
		defer span.End()
		span.SetAttributes(
			attribute.String("key", key),
			attribute.Int64("lease", lease),
		)
		createCnt.Add(ctx, 1)

		rev, err = l.backend.Create(ctx, key, value, lease)
		//TODO: why are here no error checks?
		ok = true
		span.SetAttributes(attribute.Bool("ok", ok))
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
