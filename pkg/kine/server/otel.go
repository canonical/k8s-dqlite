package server

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// TODO: This can probably get moved into tracing.go
const name = "k8s-dqlite"

var (
	tracer            = otel.Tracer(name)
	meter             = otel.Meter(name)
	backendGetCnt     metric.Int64Counter
	backendListCnt    metric.Int64Counter
	backendCountCnt   metric.Int64Counter
	backendCreateCnt  metric.Int64Counter
	backendDeleteCnt  metric.Int64Counter
	backendUpdateCnt  metric.Int64Counter
	backendCompactCnt metric.Int64Counter
)

func init() {
	ctx := context.Background()

	setupOTelSDK(ctx)
	//TODO: move this and handle shutdown setupOTelSDK

	backendListCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.list", name), metric.WithDescription("Number of list requests"))
	backendGetCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests"))
	backendCountCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.count", name), metric.WithDescription("Number of count requests"))
	backendCreateCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.create", name), metric.WithDescription("Number of create requests"))
	backendDeleteCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.delete", name), metric.WithDescription("Number of delete requests"))
	backendUpdateCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.update", name), metric.WithDescription("Number of update requests"))
	backendCompactCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.compact", name), metric.WithDescription("Number of compact requests"))
}
