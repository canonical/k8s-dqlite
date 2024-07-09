package server

import (
	"context"
	"fmt"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// TODO: This can probably get moved into tracing.go
const name = "kine"

var (
	tracer         = otel.Tracer(name)
	meter          = otel.Meter(name)
	logger         = otelslog.NewLogger(name)
	backendGetCnt  metric.Int64Counter
	backendListCnt metric.Int64Counter
)

func init() {
	ctx := context.Background()
	// otel setup
	setupOTelSDK(ctx)

	//TODO: handle shutdown

	backendListCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.list", name), metric.WithDescription("Number of list requests"))
	backendGetCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests"))
}
