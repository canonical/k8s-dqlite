package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func isDelete(txn *etcdserverpb.TxnRequest) (int64, string, bool) {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		len(txn.Failure) == 1 &&
		txn.Failure[0].GetRequestRange() != nil &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestDeleteRange() != nil {
		return txn.Compare[0].GetModRevision(), string(txn.Success[0].GetRequestDeleteRange().Key), true
	}
	return 0, "", false
}

func (l *LimitedServer) delete(ctx context.Context, key string, revision int64) (_ *etcdserverpb.TxnResponse, err error) {
	deleteCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.delete", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", key),
		attribute.Int64("revision", revision),
	)

	rev, deleted, err := l.backend.Delete(ctx, key, revision)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Bool("deleted", deleted))

	resp := &etcdserverpb.TxnResponse{
		Header:    txnHeader(rev),
		Succeeded: deleted,
	}

	if deleted {
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &etcdserverpb.DeleteRangeResponse{
						Header: txnHeader(rev),
					},
				},
			},
		}
	} else {
		rev, kv, err := l.backend.List(ctx, []byte(key), []byte(""), 1, rev)
		if err != nil {
			return nil, err
		}
		resp.Responses = []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponseRange{
					ResponseRange: &etcdserverpb.RangeResponse{
						Header: txnHeader(rev),
						Kvs:    toKVs(kv...),
					},
				},
			},
		}

	}
	return resp, nil
}
