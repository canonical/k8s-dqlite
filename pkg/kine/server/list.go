package server

import (
	"bytes"
	"context"
	"fmt"
	"strings"

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

	span.SetAttributes(
		attribute.String("key", string(r.Key)),
		attribute.String("rangeEnd", string(r.RangeEnd)),
	)
	if len(r.RangeEnd) == 0 {
		return nil, fmt.Errorf("invalid range end length of 0")
	}

	prefix := string(append(r.RangeEnd[:len(r.RangeEnd)-1], r.RangeEnd[len(r.RangeEnd)-1]-1))
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}
	start := string(bytes.TrimRight(r.Key, "\x00"))
	revision := r.Revision
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.String("start", start),
		attribute.Int64("revision", revision),
	)

	if r.CountOnly {
		rev, count, err := l.backend.Count(ctx, prefix, start, revision)
		if err != nil {
			return nil, err
		}
		span.SetAttributes(attribute.Int64("count", count))

		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, count)
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

	rev, kvs, err := l.backend.List(ctx, prefix, start, limit, revision)
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
		rev, resp.Count, err = l.backend.Count(ctx, prefix, start, revision)
		if err != nil {
			return nil, err
		}
		span.SetAttributes(attribute.Int64("count", resp.Count))

		logrus.Tracef("LIST COUNT key=%s, end=%s, revision=%d, currentRev=%d count=%d", r.Key, r.RangeEnd, revision, rev, resp.Count)
		resp.Header = txnHeader(rev)
	}

	return resp, nil
}
