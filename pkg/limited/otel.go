package limited

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "limited-server"

var (
	otelTracer trace.Tracer
	otelMeter  metric.Meter
	createCnt  metric.Int64Counter
	deleteCnt  metric.Int64Counter
	getCnt     metric.Int64Counter
	listCnt    metric.Int64Counter
	updateCnt  metric.Int64Counter
)

func init() {
	var err error
	otelTracer = otel.Tracer(otelName)
	otelMeter = otel.Meter(otelName)

	createCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.create", otelName), metric.WithDescription("Number of create requests"))
	if err != nil {
		createCnt = noop.Int64Counter{}
		logrus.WithError(err).Warning("otel failed to create create counter")
	}
	deleteCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.delete", otelName), metric.WithDescription("Number of delete requests"))
	if err != nil {
		deleteCnt = noop.Int64Counter{}
		logrus.WithError(err).Warning("otel failed to create delete counter")
	}
	getCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.get", otelName), metric.WithDescription("Number of get requests"))
	if err != nil {
		getCnt = noop.Int64Counter{}
		logrus.WithError(err).Warning("otel failed to create get counter")
	}
	listCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.list", otelName), metric.WithDescription("Number of list requests"))
	if err != nil {
		listCnt = noop.Int64Counter{}
		logrus.WithError(err).Warning("otel failed to create list counter")
	}
	updateCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.update", otelName), metric.WithDescription("Number of update requests"))
	if err != nil {
		updateCnt = noop.Int64Counter{}
		logrus.WithError(err).Warning("otel failed to create update counter")
	}
}
