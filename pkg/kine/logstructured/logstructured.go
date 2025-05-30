package logstructured

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/cenkalti/backoff/v4"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "logstructured"

var (
	otelTracer trace.Tracer
)

func init() {
	otelTracer = otel.Tracer(otelName)
}

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
	log Log
	wg  sync.WaitGroup
}

func New(log Log) *LogStructured {
	return &LogStructured{
		log: log,
	}
}

func (l *LogStructured) DoCompact(ctx context.Context) error {
	return l.log.DoCompact(ctx)
}

func (l *LogStructured) Start(ctx context.Context) error {
	if err := l.log.Start(ctx); err != nil {
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

func (l *LogStructured) Wait() {
	l.wg.Wait()
	l.log.Wait()
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
	rev, events, err := l.log.List(ctx, key, rangeEnd, limit, revision, includeDeletes)
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

func (l *LogStructured) Create(ctx context.Context, key string, value []byte, lease int64) (rev int64, created bool, err error) {
	rev, created, err = l.log.Create(ctx, key, value, lease)
	logrus.Debugf("CREATE %s, size=%d, lease=%d => rev=%d, err=%v", key, len(value), lease, rev, err)
	return rev, created, err
}

func (l *LogStructured) Delete(ctx context.Context, key string, revision int64) (revRet int64, deleted bool, errRet error) {
	rev, del, err := l.log.Delete(ctx, key, revision)
	logrus.Debugf("DELETE %s, rev=%d => rev=%d, deleted=%v, err=%v", key, revision, rev, del, err)
	return rev, del, err
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

	rev, events, err := l.log.List(ctx, prefix, startKey, limit, revision, false)
	if err != nil {
		return 0, nil, err
	}

	kvs := make([]*server.KeyValue, len(events))
	for i, event := range events {
		kvs[i] = event.KV
	}
	return rev, kvs, nil
}

func (l *LogStructured) Count(ctx context.Context, prefix, startKey string, revision int64) (revRet int64, count int64, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Count", otelName))
	defer func() {
		logrus.Debugf("COUNT prefix=%s startKey=%s => rev=%d, count=%d, err=%v", prefix, startKey, revRet, count, err)
		span.SetAttributes(
			attribute.String("prefix", prefix),
			attribute.String("startKey", startKey),
			attribute.Int64("revision", revision),
			attribute.Int64("current-revision", revRet),
			attribute.Int64("count", count),
		)
		span.RecordError(err)
		span.End()
	}()
	return l.log.Count(ctx, prefix, startKey, revision)
}

func (l *LogStructured) Update(ctx context.Context, key string, value []byte, revision, lease int64) (revRet int64, updateRet bool, errRet error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Update", otelName))
	defer func() {
		logrus.Debugf("UPDATE %s, value=%d, rev=%d, lease=%v => rev=%d, updated=%v, err=%v", key, len(value), revision, lease, revRet, updateRet, errRet)
		span.SetAttributes(
			attribute.String("key", key),
			attribute.Int64("revision", revision),
			attribute.Int64("lease", lease),
			attribute.Int64("value-size", int64(len(value))),
			attribute.Int64("current-revision", revRet),
			attribute.Bool("updated", updateRet),
		)
		span.End()
	}()
	return l.log.Update(ctx, key, value, revision, lease)
}

func (l *LogStructured) ttlEvents(ctx context.Context) chan *server.Event {
	result := make(chan *server.Event)
	var shouldClose atomic.Bool

	l.wg.Add(2)
	go func() {
		defer l.wg.Done()

		rev, events, err := l.log.List(ctx, "/", "", 1000, 0, false)
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

			_, events, err = l.log.List(ctx, "/", events[len(events)-1].KV.Key, 1000, rev, false)
		}

		if !shouldClose.CompareAndSwap(false, true) {
			close(result)
		}
	}()

	go func() {
		defer l.wg.Done()

		for events := range l.log.Watch(ctx, "/") {
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
			// add some jitter to avoid ttl expiry and deletion on all nodes at the same time with the goal of
			// one of them succeeding if the DB is under high load (busy)
			jitterDelay := time.Duration(rand.Intn(1000)) * time.Millisecond // Jitter up to 1 second as api-server does not expect sub-second precision
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Duration(event.KV.Lease)*time.Second + jitterDelay):
			}

			deleteLease := func() error {
				// retry until we succeed or context is cancelled, if another node has already deleted the key the delete will not return an error and we will exit the retry loop
				mutex.Lock()
				rev, deleted, err := l.Delete(ctx, event.KV.Key, event.KV.ModRevision)
				mutex.Unlock()
				if err != nil {
					logrus.Errorf("failed to delete %s for TTL: %v, retrying", event.KV.Key, err)
					return err
				}
				if deleted {
					logrus.Debugf("deleted %s for TTL, revRet=%d", event.KV.Key, rev)
				}
				return nil
			}

			b := backoff.NewExponentialBackOff()
			b.InitialInterval = 100 * time.Millisecond
			b.RandomizationFactor = 0.5
			b.Multiplier = 1.5
			b.MaxInterval = 5 * time.Second
			b.MaxElapsedTime = 0 // retry indefinitely
			b.Reset()

			err := backoff.Retry(deleteLease, backoff.WithContext(b, ctx))
			if err != nil {
				logrus.Errorf("failed to delete %s for TTL after retries: %v", event.KV.Key, err)
			}
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
	readChan := l.log.Watch(ctx, prefix)

	// include the current revision in list
	if revision > 0 {
		revision -= 1
	}

	result := make(chan []*server.Event, 100)

	rev, kvs, err := l.log.After(ctx, prefix, revision, 0)
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
			result <- filter(i, lastRevision)
		}
		close(result)
		cancel()
	}()

	return result
}

func filter(events []*server.Event, rev int64) []*server.Event {
	for len(events) > 0 && events[0].KV.ModRevision <= rev {
		events = events[1:]
	}

	return events
}

func (l *LogStructured) DbSize(ctx context.Context) (int64, error) {
	return l.log.DbSize(ctx)
}
