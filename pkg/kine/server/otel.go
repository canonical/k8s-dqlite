package server

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// TODO: This can probably get moved into tracing.go
const name = "k8s-dqlite"

var (
	tracer     = otel.Tracer(name)
	meter      = otel.Meter(name)
	getCnt     metric.Int64Counter
	listCnt    metric.Int64Counter
	countCnt   metric.Int64Counter
	createCnt  metric.Int64Counter
	deleteCnt  metric.Int64Counter
	updateCnt  metric.Int64Counter
	compactCnt metric.Int64Counter
)

func init() {
	listCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.list", name), metric.WithDescription("Number of list requests"))
	getCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests"))
	countCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.count", name), metric.WithDescription("Number of count requests"))
	createCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.create", name), metric.WithDescription("Number of create requests"))
	deleteCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.delete", name), metric.WithDescription("Number of delete requests"))
	updateCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.update", name), metric.WithDescription("Number of update requests"))
	compactCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.compact", name), metric.WithDescription("Number of compact requests"))
}
