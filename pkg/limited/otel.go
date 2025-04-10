package limited

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "limited-server"

var (
	otelTracer      trace.Tracer
	otelMeter       metric.Meter
	createCnt       metric.Int64Counter
	deleteCnt       metric.Int64Counter
	getCnt          metric.Int64Counter
	listCnt         metric.Int64Counter
	updateCnt       metric.Int64Counter
	compactCnt      metric.Int64Counter
	watcherGroupCnt metric.Int64Counter
)

func init() {
	var err error
	otelTracer = otel.Tracer(otelName)
	otelMeter = otel.Meter(otelName)

	createCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.create", otelName), metric.WithDescription("Number of create requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	deleteCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.delete", otelName), metric.WithDescription("Number of delete requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create delete counter")
	}
	getCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.get", otelName), metric.WithDescription("Number of get requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create get counter")
	}
	listCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.list", otelName), metric.WithDescription("Number of list requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create list counter")
	}
	updateCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.update", otelName), metric.WithDescription("Number of update requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create update counter")
	}
	compactCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.compact", otelName), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	watcherGroupCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.watcherGroup", otelName), metric.WithDescription("Number of watcherGroup requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
}
