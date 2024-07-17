package server

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	createCnt metric.Int64Counter
)

func init() {
	var err error

	createCnt, err = meter.Int64Counter(fmt.Sprintf("%s.create", name), metric.WithDescription("Number of create requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
}

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
	createCnt.Add(ctx, 1)
	ctx, span := tracer.Start(ctx, "limited.create")
	defer span.End()
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

	rev, err := l.backend.Create(ctx, string(put.Key), put.Value, put.Lease)
	span.SetAttributes(attribute.Int64("revision", rev))
	if err == ErrKeyExists {
		span.AddEvent("key exists")
		return &etcdserverpb.TxnResponse{
			Header:    txnHeader(rev),
			Succeeded: false,
		}, nil
	} else if err != nil {
		defer span.RecordError(err)
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
