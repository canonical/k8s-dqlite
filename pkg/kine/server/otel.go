package server

import (
	"fmt"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const name = "limited-server"

var (
	tracer    = otel.Tracer(name)
	meter     = otel.Meter(name)
	getCnt    metric.Int64Counter
	listCnt   metric.Int64Counter
	countCnt  metric.Int64Counter
	createCnt metric.Int64Counter
	deleteCnt metric.Int64Counter
	updateCnt metric.Int64Counter
)

func init() {
	var err error

	listCnt, err = meter.Int64Counter(fmt.Sprintf("%s.list", name), metric.WithDescription("Number of list requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create list counter")
	}

	getCnt, err = meter.Int64Counter(fmt.Sprintf("%s.get", name), metric.WithDescription("Number of get requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create get counter")
	}

	countCnt, err = meter.Int64Counter(fmt.Sprintf("%s.count", name), metric.WithDescription("Number of count requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create count counter")
	}

	createCnt, err = meter.Int64Counter(fmt.Sprintf("%s.create", name), metric.WithDescription("Number of create requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}

	deleteCnt, err = meter.Int64Counter(fmt.Sprintf("%s.delete", name), metric.WithDescription("Number of delete requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create delete counter")
	}

	updateCnt, err = meter.Int64Counter(fmt.Sprintf("%s.update", name), metric.WithDescription("Number of update requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create update counter")
	}

}
