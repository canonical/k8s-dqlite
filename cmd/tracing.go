package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	resourceName = "k8s-dqlite"
)

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context, otelEndpoint string) (shutdown func(context.Context) error, err error) {
	conn, err := initConn(otelEndpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(resourceName),
		),
	)
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create resource")
	}

	traceExporter, err := newTraceExporter(ctx, conn)
	if err != nil {
		connErr := conn.Close()
		if connErr != nil {
			logrus.WithError(connErr).Warning("Failed to shut down otel gRPC connection")
		}
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	tracerProvider := newTraceProvider(traceExporter, res)
	otel.SetTracerProvider(tracerProvider)

	meterProvider, err := newMeterProvider(res)
	if err != nil {
		var shutdownErrs error
		shutdownErr := tracerProvider.Shutdown(ctx)
		if shutdownErr != nil {
			shutdownErrs = errors.Join(shutdownErrs, shutdownErr)
		}
		shutdownErr = conn.Close()
		if shutdownErr != nil {
			shutdownErrs = errors.Join(shutdownErrs, shutdownErr)
		}
		if shutdownErrs != nil {
			logrus.WithError(shutdownErrs).Warning("Failed to shutdown OpenTelemetry SDK")
			return nil, fmt.Errorf("failed to create meter provider: %w", err)
		}
	}
	otel.SetMeterProvider(meterProvider)

	shutdown = func(ctx context.Context) error {
		var shutdownErrs error
		err = meterProvider.Shutdown(ctx)
		if err != nil {
			shutdownErrs = errors.Join(shutdownErrs, err)
		}
		err = tracerProvider.Shutdown(ctx)
		if err != nil {
			shutdownErrs = errors.Join(shutdownErrs, err)
		}
		err = conn.Close()
		if err != nil {
			shutdownErrs = errors.Join(shutdownErrs, err)
		}
		return shutdownErrs
	}
	return shutdown, nil
}

func initConn(otelEndpoint string) (*grpc.ClientConn, error) {
	// It connects the OpenTelemetry Collector through local gRPC connection.
	conn, err := grpc.NewClient(otelEndpoint,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	return conn, nil
}

func newTraceExporter(ctx context.Context, conn *grpc.ClientConn) (trace.SpanExporter, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}
	return exporter, nil
}

func newTraceProvider(traceExporter trace.SpanExporter, res *resource.Resource) *trace.TracerProvider {
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter,
			trace.WithBatchTimeout(time.Second),
		),
		trace.WithResource(res),
	)
	return traceProvider
}

func newMeterProvider(res *resource.Resource) (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			metric.WithInterval(30*time.Second))),
		metric.WithResource(res),
	)
	return meterProvider, nil
}
