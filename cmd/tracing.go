package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"path/filepath"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	resourceName = "k8s-dqlite"
)

// NameFilter is a SpanProcessor that filters trace spans based on the span name.
type NameFilter struct {
	// Next is the next SpanProcessor in the chain.
	Next trace.SpanProcessor

	// AllowedPattern is a regex that will be used to filter spans,
	// dropping the the spans in case of mismatching span names.
	AllowedPattern *regexp.Regexp
}

func (f NameFilter) OnStart(parent context.Context, s trace.ReadWriteSpan) {
	f.Next.OnStart(parent, s)
}
func (f NameFilter) Shutdown(ctx context.Context) error   { return f.Next.Shutdown(ctx) }
func (f NameFilter) ForceFlush(ctx context.Context) error { return f.Next.ForceFlush(ctx) }
func (f NameFilter) OnEnd(s trace.ReadOnlySpan) {
	if !f.AllowedPattern.MatchString(s.Name()) {
		return
	}
	f.Next.OnEnd(s)
}
// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(ctx context.Context, otelEndpoint string, otelDir string, spanFilter string) (shutdown func(context.Context) error, err error) {
	var grpcConn *grpc.ClientConn
	var traceExporter trace.SpanExporter
	var traceProvider *trace.TracerProvider
	var metricExporter sdkmetric.Exporter
	var meterProvider *metric.MeterProvider
	var metricFile *os.File
	var traceFile *os.File

	shutdown = func(ctx context.Context) error {
		var shutdownErrs error
		if meterProvider != nil {
			err = meterProvider.Shutdown(ctx)
			if err != nil {
				logrus.WithError(err).Warning("Failed to shut down otel meter provider")
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if traceProvider != nil {
			err = traceProvider.Shutdown(ctx)
			if err != nil {
				logrus.WithError(err).Warning("Failed to shut down otel trace provider")
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if grpcConn != nil {
			err = grpcConn.Close()
			if err != nil {
				logrus.WithError(err).Warning("Failed to shut down otel gRPC connection")
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if metricFile != nil {
			err = metricFile.Close()
			if err != nil {
				logrus.WithError(err).Warning("Failed to close otel meter file")
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if traceFile != nil {
			err = traceFile.Close()
			if err != nil {
				logrus.WithError(err).Warning("Failed to close otel trace file")
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		return shutdownErrs
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(resourceName),
		),
	)
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create resource")
		return nil, err
	}

	if otelDir != "" {
		traceFilePath := filepath.Join(otelDir, "k8s-dqlite-traces.txt")
		traceFile, err = os.OpenFile(traceFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			shutdown(ctx)
			return nil, fmt.Errorf("failed to open otel trace file %s: %w", traceFilePath, err)
		}
		metricFilePath := filepath.Join(otelDir, "k8s-dqlite-metrics.txt")
		metricFile, err = os.OpenFile(metricFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			shutdown(ctx)
			return nil, fmt.Errorf("failed to open otel metric file %s: %w", traceFilePath, err)
		}
	} else {
		grpcConn, err = initConn(otelEndpoint)
		if err != nil {
			shutdown(ctx)
			return nil, err
		}
	}

	// Initialize trace exporter.
	if otelDir != "" {
		traceExporter, err = newFileTraceExporter(ctx, traceFile)
	} else {
		traceExporter, err = newGrpcTraceExporter(ctx, grpcConn)
	}
	if err != nil {
		shutdown(ctx)
		return nil, err
	}

	tracerProvider, err := newTraceProvider(traceExporter, res, spanFilter)
	if err != nil {
		shutdown(ctx)
		return nil, err
	}
	otel.SetTracerProvider(tracerProvider)

	// Initialize meter exporter.
	if otelDir != "" {
		metricExporter, err = newFileMetricExporter(ctx, metricFile)
	} else {
		metricExporter, err = newGrpcMetricExporter(ctx, grpcConn)
	}
	if err != nil {
		shutdown(ctx)
		return nil, err
	}

	meterProvider, err = newMeterProvider(metricExporter, res)
	if err != nil {
		shutdown(ctx)
		return nil, err
	}
	otel.SetMeterProvider(meterProvider)

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

func newGrpcTraceExporter(ctx context.Context, conn *grpc.ClientConn) (trace.SpanExporter, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC trace exporter: %w", err)
	}
	return exporter, nil
}

func newFileTraceExporter(ctx context.Context, file *os.File) (trace.SpanExporter, error) {
	exporter, err := stdouttrace.New(
		// stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(file),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create file trace exporter: %w", err)
	}
	return exporter, nil
}

func newTraceProvider(traceExporter trace.SpanExporter, res *resource.Resource, spanFilter string) (*trace.TracerProvider, error) {
	var sp sdktrace.SpanProcessor
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	sp = bsp

	if spanFilter != "" {
		allowedPattern, err := regexp.Compile(spanFilter)
		if err != nil {
			return nil, fmt.Errorf("invalid span filter regex %s: %w", spanFilter, err)
		}
		filter := NameFilter{
			Next: bsp,
			AllowedPattern: allowedPattern,
		}
		sp = filter
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(sp),
	)
	return traceProvider, nil
}

func newGrpcMetricExporter(ctx context.Context, conn *grpc.ClientConn) (sdkmetric.Exporter, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}
	return metricExporter, nil
}

func newFileMetricExporter(ctx context.Context, file *os.File) (sdkmetric.Exporter, error) {
	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
		stdoutmetric.WithWriter(file),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric exporter: %w", err)
	}
	return metricExporter, nil
}

func newMeterProvider(metricExporter sdkmetric.Exporter, res *resource.Resource) (*metric.MeterProvider, error) {
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	return meterProvider, nil
}
