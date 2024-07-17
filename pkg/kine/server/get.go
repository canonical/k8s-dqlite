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
	getCnt metric.Int64Counter
)

func init() {
	var err error
	getCnt, err = meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create get counter")
	}
}

func (l *LimitedServer) get(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	getCnt.Add(ctx, 1)
	ctx, span := tracer.Start(ctx, "limited.get")
	defer span.End()

	span.SetAttributes(
		attribute.String("key", string(r.Key)),
		attribute.String("rangeEnd", string(r.RangeEnd)),
		attribute.Int64("limit", r.Limit),
		attribute.Int64("revision", r.Revision),
	)
	if r.Limit != 0 && len(r.RangeEnd) != 0 {
		err := fmt.Errorf("invalid combination of rangeEnd and limit, limit should be 0 got %d", r.Limit)
		span.RecordError(err)
		return nil, err
	}

	rev, kv, err := l.backend.Get(ctx, string(r.Key), string(r.RangeEnd), r.Limit, r.Revision)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	span.End()

	resp := &RangeResponse{
		Header: txnHeader(rev),
	}
	if kv != nil {
		resp.Kvs = []*KeyValue{kv}
	}
	return resp, nil
}
