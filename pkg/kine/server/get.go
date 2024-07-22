package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	var err error
	getCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.get", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	span.SetAttributes(
		attribute.String("key", string(r.Key)),
		attribute.String("rangeEnd", string(r.RangeEnd)),
		attribute.Int64("limit", r.Limit),
		attribute.Int64("revision", r.Revision),
	)
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		err := fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
		return nil, err
	}

	rev, kv, err := l.backend.Get(ctx, string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)
	if err != nil {
		return nil, err
	}

	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
	}
	return resp, nil
}
