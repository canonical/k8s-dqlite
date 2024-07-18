package generic

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const name = "generic"

var (
	tracer           = otel.Tracer(name)
	meter            = otel.Meter(name)
	setCompactRevCnt metric.Int64Counter
	getRevisionCnt   metric.Int64Counter
	deleteRevCnt     metric.Int64Counter
	currentRevCnt    metric.Int64Counter
	getCompactRevCnt metric.Int64Counter
)

func init() {
	var err error
	setCompactRevCnt, err = meter.Int64Counter(fmt.Sprintf("%s.compact", name), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	getRevisionCnt, err = meter.Int64Counter(fmt.Sprintf("%s.get_revision", name), metric.WithDescription("Number of get revision requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	deleteRevCnt, err = meter.Int64Counter(fmt.Sprintf("%s.delete_revision", name), metric.WithDescription("Number of delete revision requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	currentRevCnt, err = meter.Int64Counter(fmt.Sprintf("%s.current_revision", name), metric.WithDescription("Current revision"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	getCompactRevCnt, err = meter.Int64Counter(fmt.Sprintf("%s.get_compact_revision", name), metric.WithDescription("Get compact revision"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}

}
