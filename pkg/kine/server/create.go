package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func isCreate(txn *etcdserverpb.TxnRequest) *etcdserverpb.PutRequest {
	if len(txn.Compare) == 1 &&
		txn.Compare[0].Target == etcdserverpb.Compare_MOD &&
		txn.Compare[0].Result == etcdserverpb.Compare_EQUAL &&
		txn.Compare[0].GetModRevision() == 0 &&
		len(txn.Failure) == 0 &&
		len(txn.Success) == 1 &&
		txn.Success[0].GetRequestPut() != nil {
		return txn.Success[0].GetRequestPut()
	}
	return nil
}

func (l *LimitedServer) create(ctx context.Context, put *etcdserverpb.PutRequest, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if put.IgnoreValue {
		return nil, unsupported("ignoreValue")
	} else if put.PrevKv {
		return nil, unsupported("prevKv")
	}

	ctx, span := tracer.Start(ctx, "backend.create")
	defer span.End()

	span.SetAttributes(

		attribute.String("key", string(put.Key)),
		//TODO: get key and value in utf-8 string
		// attribute.String("value", string(put.Value)),
		attribute.Int64("lease", put.Lease),
	)
	backendCreateCnt.Add(ctx, 1)

	rev, err := l.backend.Create(ctx, string(put.Key), put.Value, put.Lease)
	if err == ErrKeyExists {
		span.RecordError(err)
		return &etcdserverpb.TxnResponse{
			Header:    txnHeader(rev),
			Succeeded: false,
		}, nil
	} else if err != nil {
		span.RecordError(err)
		return nil, err
	}

	return &etcdserverpb.TxnResponse{
		Header: txnHeader(rev),
		Responses: []*etcdserverpb.ResponseOp{
			{
				Response: &etcdserverpb.ResponseOp_ResponsePut{
					ResponsePut: &etcdserverpb.PutResponse{
						Header: txnHeader(rev),
					},
				},
			},
		},
		Succeeded: true,
	}, nil
}
