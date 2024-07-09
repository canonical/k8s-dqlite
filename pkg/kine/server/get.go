package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		return nil, fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
	}

	ctx, span := tracer.Start(
		ctx,
		"backend.get",
	)

	span.SetAttributes(
		attribute.String("key", string(r.Key)),
		attribute.Int64("limit", int64(r.Limit)),
		attribute.Int64("revision", r.Revision),
	)

	defer span.End()
	getCnt.Add(ctx, 1)

	rev, kv, err := l.backend.Get(ctx, string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.AddEvent("get request is completed")
	span.End()

	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
	}
	return resp, nil
}
