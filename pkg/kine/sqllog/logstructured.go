package sqllog

import (
	"context"
	"fmt"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type Log interface {
	Start(ctx context.Context) error
	Wait()
	CurrentRevision(ctx context.Context) (int64, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeletes bool) (int64, []*server.Event, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (rev int64, created bool, err error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (rev int64, updated bool, err error)
	Delete(ctx context.Context, key string, revision int64) (rev int64, deleted bool, err error)
	After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error)
	Watch(ctx context.Context, prefix string) <-chan []*server.Event
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	DbSize(ctx context.Context) (int64, error)
	DoCompact(ctx context.Context) error
}

type LogStructured struct {
	*SQLLog
}

func (l *LogStructured) Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (revRet int64, kvRet *server.KeyValue, errRet error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Get", otelName))
	span.SetAttributes(
		attribute.String("key", key),
		attribute.String("rangeEnd", rangeEnd),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
	)
	defer func() {
		logrus.Debugf("GET %s, rev=%d => rev=%d, kv=%v, err=%v", key, revision, revRet, kvRet != nil, errRet)
		span.SetAttributes(attribute.Int64("current-revision", revRet))
		span.RecordError(errRet)
		span.End()
	}()

	rev, event, err := l.get(ctx, key, rangeEnd, limit, revision, false)
	if event == nil {
		return rev, nil, err
	}
	return rev, event.KV, err
}

func (l *LogStructured) get(ctx context.Context, key, rangeEnd string, limit, revision int64, includeDeletes bool) (int64, *server.Event, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.get", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", key),
		attribute.String("rangeEnd", rangeEnd),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
		attribute.Bool("includeDeletes", includeDeletes),
	)
	rev, events, err := l.SQLLog.List(ctx, key, rangeEnd, limit, revision, includeDeletes)
	if err == server.ErrCompacted {
		span.AddEvent("key already compacted")
		// ignore compacted when getting by revision
		err = nil
	} else if err != nil {
		return 0, nil, err
	}
	if revision != 0 {
		rev = revision
	}
	if len(events) == 0 {
		return rev, nil, nil
	}
	return rev, events[0], nil
}

func (l *LogStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (_ int64, _ []*server.KeyValue, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.List", otelName))
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.String("startKey", startKey),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
	)

	defer func() {
		// logrus.Debugf("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
		span.RecordError(err)
		span.End()
	}()

	rev, events, err := l.SQLLog.List(ctx, prefix, startKey, limit, revision, false)
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, len(events))
	for i, event := range events {
		kvs[i] = event.KV
	}
	return rev, kvs, nil
}
