package server

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const name = "kine"

var (
	tracer = otel.Tracer(name)
	meter  = otel.Meter(name)
	logger = otelslog.NewLogger(name)
	getCnt metric.Int64Counter
)

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		return nil, fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
	}

	ctx, span := tracer.Start(
		ctx,
		"Backend.get",
		// trace.WithAttributes(
		// 	trace.StringAttribute("key", string(r.Key)),
		// 	trace.StringAttribute("rangeEnd", string(r.RangeEnd)),
		// 	trace.Int64Attribute("limit", int64(r.Limit)),
		// 	trace.Int64Attribute("revision", r.Revision),
		// ),
	)

	defer span.End()

	getCnt, err := meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests")) // TODO: move this init out of the function
	if err != nil {
		span.RecordError(err)
	}

	span.SetAttributes()

	getCnt.Add(ctx, 1)
	logger.InfoContext(ctx, fmt.Sprintf("get request number %d", getCnt))

	rev, kv, err := l.backend.Get(ctx, string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)
	if err != nil {
		span.RecordError(err)
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
