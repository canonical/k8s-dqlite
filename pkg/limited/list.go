package limited

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

func (l *LimitedServer) list(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	var err error
	listCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.list", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	revision := r.Revision
	span.SetAttributes(
		attribute.String("key", string(r.Key)),
		attribute.String("rangeEnd", string(r.RangeEnd)),
		attribute.Int64("revision", r.Revision),
	)
	if len(r.RangeEnd) == 0 {
		return nil, fmt.Errorf("invalid range end length of 0")
	}

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, r.Key, r.RangeEnd, revision)
		if err != nil {
			return nil, err
		}
		span.SetAttributes(attribute.Int64("count", count))

		logrus.Tracef("LIST COUNT key=%s, r.RangeEnd=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, count)
		return &RangeResponse{
			Header: txnHeader(rev),
			Count:  count,
		}, nil
	}

	limit := r.Limit
	if limit > 0 {
		limit++
	}
	span.SetAttributes(attribute.Int64("limit", limit))

	rev, kvs, err := l.backend.List(ctx, r.Key, r.RangeEnd, limit, revision)
	if err != nil {
		return nil, err
	}

	resp := &RangeResponse{
		Header: txnHeader(rev),
		Count:  int64(len(kvs)),
		Kvs:    kvs,
	}
	span.SetAttributes(attribute.Int64("list-count", resp.Count))

	// count the actual number of results if there are more items in the db.
	if limit > 0 && resp.Count > r.Limit {
		resp.More = true
		resp.Kvs = kvs[0 : limit-1]

		if revision == 0 {
			revision = rev
		}

		// count the actual number of results if there are more items in the db.
		rev, resp.Count, err = l.backend.Count(ctx, r.Key, r.RangeEnd, revision)
		if err != nil {
			return nil, err
		}

		span.SetAttributes(attribute.Int64("count", resp.Count))
		logrus.Tracef("LIST COUNT key=%s, r.RangeEnd=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, resp.Count)
		resp.Header = txnHeader(rev)
	}

	return resp, nil
}
