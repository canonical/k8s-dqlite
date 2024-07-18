package sqllog

import (
	"fmt"

	"github.com/sirupsen/logrus"
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
	var err error
	compactCnt, err = meter.Int64Counter(fmt.Sprintf("%s.compact", name), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}

}
