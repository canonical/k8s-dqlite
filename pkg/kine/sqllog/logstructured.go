package sqllog

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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

func (l *LogStructured) Start(ctx context.Context) error {
	if err := l.SQLLog.Start(ctx); err != nil {
		return err
	}
	l.Create(ctx, "/registry/health", []byte(`{"health":"true"}`), 0)

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.ttl(ctx)
	}()
	return nil
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

func (l *LogStructured) List(ctx context.Context, prefix, startKey string, limit, revision int64) (revRet int64, kvRet []*server.KeyValue, errRet error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.List", otelName))
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.String("startKey", startKey),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
	)

	defer func() {
		logrus.Debugf("LIST %s, start=%s, limit=%d, rev=%d => rev=%d, kvs=%d, err=%v", prefix, startKey, limit, revision, revRet, len(kvRet), errRet)
		span.RecordError(errRet)
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

func (l *LogStructured) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)
	var shouldClose atomic.Bool

	l.wg.Add(2)
	go func() {
		defer l.wg.Done()

		rev, events, err := l.SQLLog.List(ctx, "/", "", 1000, 0, false)
		for len(events) > 0 {
			if err != nil {
				logrus.Errorf("failed to read old events for ttl: %v", err)
				return
			}

			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}

			_, events, err = l.SQLLog.List(ctx, "/", events[len(events)-1].KV.Key, 1000, rev, false)
		}

		if !shouldClose.CompareAndSwap(false, true) {
			close(result)
		}
	}()

	go func() {
		defer l.wg.Done()

		for events := range l.SQLLog.Watch(ctx, "/") {
			for _, event := range events {
				if event.KV.Lease > 0 {
					result <- event
				}
			}
		}

		if !shouldClose.CompareAndSwap(false, true) {
			close(result)
		}
	}()

	return result
}

func (l *LogStructured) ttl(ctx context.Context) {
	// very naive TTL support
	mutex := &sync.Mutex{}
	for event := range l.ttlEvents(ctx) {
		go func(event *server.Event) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(event.KV.Lease) * time.Second):
			}
			mutex.Lock()
			l.Delete(ctx, event.KV.Key, event.KV.ModRevision)
			mutex.Unlock()
		}(event)
	}
}

func (l *LogStructured) Watch(ctx context.Context, prefix string, revision int64) <-chan []*server.Event {
	logrus.Debugf("WATCH %s, revision=%d", prefix, revision)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Watch", otelName))
	defer span.End()
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.Int64("revision", revision),
	)

	// starting watching right away so we don't miss anything
	ctx, cancel := context.WithCancel(ctx)
	readChan := l.SQLLog.Watch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision -= 1
	}

	result := make(chan []*server.Event, 100)

	rev, kvs, err := l.SQLLog.After(ctx, prefix, revision, 0)
	if err != nil {
		logrus.Errorf("failed to list %s for revision %d", prefix, revision)
		msg := fmt.Sprintf("failed to list %s for revision %d", prefix, revision)
		span.AddEvent(msg)
		logrus.Errorf(msg)
		cancel()
	}

	logrus.Debugf("WATCH LIST key=%s rev=%d => rev=%d kvs=%d", prefix, revision, rev, len(kvs))
	span.SetAttributes(attribute.Int64("current-revision", rev), attribute.Int64("kvs-count", int64(len(kvs))))

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		lastRevision := revision
		if len(kvs) > 0 {
			lastRevision = rev
		}

		if len(kvs) > 0 {
			result <- kvs
		}

		// always ensure we fully read the channel
		for i := range readChan {
			result <- filterRevision(i, lastRevision)
		}
		close(result)
		cancel()
	}()

	return result
}

func filterRevision(events []*server.Event, rev int64) []*server.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}
