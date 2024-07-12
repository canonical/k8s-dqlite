package sqllog

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const name = "sqllog"

var (
	tracer     = otel.Tracer(name)
	meter      = otel.Meter(name)
	compactCnt metric.Int64Counter
)

func init() {
	compactCnt, _ = meter.Int64Counter(fmt.Sprintf("%s.compact", name), metric.WithDescription("Number of compact requests"))
}
