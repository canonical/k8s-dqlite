package limited

import (
	"context"
	"fmt"

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

func (l *LimitedServer) create(ctx context.Context, put *etcdserverpb.PutRequest) (*etcdserverpb.TxnResponse, error) {
	var err error
	createCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.create", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(put.Key)),
		attribute.Int64("lease", put.Lease),
	)

	if put.IgnoreLease {
		return nil, unsupported("ignoreLease")
	} else if put.IgnoreValue {
		return nil, unsupported("ignoreValue")
	} else if put.PrevKv {
		return nil, unsupported("prevKv")
	}

	rev, created, err := l.backend.Create(ctx, put.Key, put.Value, put.Lease)
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.Int64("revision", rev))
	if !created {
		span.AddEvent("key exists")
		return &etcdserverpb.TxnResponse{
			Header:    txnHeader(rev),
			Succeeded: false,
		}, nil
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
