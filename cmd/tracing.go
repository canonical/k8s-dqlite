package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

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
	if f.AllowedPattern != nil && !f.AllowedPattern.MatchString(s.Name()) {
		return
	}
	f.Next.OnEnd(s)
}

// DurationFilter is a SpanProcessor that filters spans that have lifetimes
// outside of a defined range.
type DurationFilter struct {
	// Next is the next SpanProcessor in the chain.
	Next trace.SpanProcessor

	// Min is the duration under which spans are dropped.
	Min time.Duration
}

func (f DurationFilter) OnStart(parent context.Context, s trace.ReadWriteSpan) {
	f.Next.OnStart(parent, s)
}
func (f DurationFilter) Shutdown(ctx context.Context) error   { return f.Next.Shutdown(ctx) }
func (f DurationFilter) ForceFlush(ctx context.Context) error { return f.Next.ForceFlush(ctx) }
func (f DurationFilter) OnEnd(s trace.ReadOnlySpan) {
	if f.Min > 0 && s.EndTime().Sub(s.StartTime()) < f.Min {
		// Drop short lived spans.
		return
	}
	f.Next.OnEnd(s)
}

// setupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func setupOTelSDK(
	ctx context.Context, otelEndpoint string,
	otelDir string, spanNameFilter string,
	spanMinDurationFilter time.Duration) (shutdown func(context.Context) error) {
	var grpcConn *grpc.ClientConn
	var traceExporter trace.SpanExporter
	var traceProvider *trace.TracerProvider
	var metricExporter sdkmetric.Exporter
	var meterProvider *metric.MeterProvider
	var metricFile *os.File
	var traceFile *os.File
	var err error

	defer func() {
		logrus.WithError(err).Warning("failed to setup otel sdk")
	}()

	shutdown = func(ctx context.Context) error {
		var shutdownErrs error
		if meterProvider != nil {
			err := meterProvider.Shutdown(ctx)
			if err != nil {
				err = fmt.Errorf("failed to shut down otel meter provider: %w", err)
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if traceProvider != nil {
			err := traceProvider.Shutdown(ctx)
			if err != nil {
				err = fmt.Errorf("failed to shut down otel trace provider: %w", err)
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if grpcConn != nil {
			err := grpcConn.Close()
			if err != nil {
				err = fmt.Errorf("failed to shut down otel grpc connection: %w", err)
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if metricFile != nil {
			err := metricFile.Close()
			if err != nil {
				err = fmt.Errorf("failed to close otel meter file: %w", err)
				shutdownErrs = errors.Join(shutdownErrs, err)
			}
		}
		if traceFile != nil {
			err := traceFile.Close()
			if err != nil {
				err = fmt.Errorf("failed to close otel trace file: %w", err)
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
		err = fmt.Errorf("otel failed to create resource: %w", err)
		return nil
	}

	if otelDir != "" {
		traceFilePath := filepath.Join(otelDir, "k8s-dqlite-traces.txt")
		traceFile, err = os.OpenFile(traceFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			shutdown(ctx)
			err = fmt.Errorf("failed to open otel trace file %s: %w", traceFilePath, err)
			return nil
		}
		metricFilePath := filepath.Join(otelDir, "k8s-dqlite-metrics.txt")
		metricFile, err = os.OpenFile(metricFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			shutdown(ctx)
			err = fmt.Errorf("failed to open otel metric file %s: %w", traceFilePath, err)
			return nil
		}
	} else {
		grpcConn, err = initConn(otelEndpoint)
		if err != nil {
			shutdown(ctx)
			return nil
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
		return nil
	}

	tracerProvider, err := newTraceProvider(traceExporter, res, spanNameFilter, spanMinDurationFilter)
	if err != nil {
		shutdown(ctx)
		return nil
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
		return nil
	}

	meterProvider, err = newMeterProvider(metricExporter, res)
	if err != nil {
		shutdown(ctx)
		return nil
	}
	otel.SetMeterProvider(meterProvider)

	return shutdown
}

func initConn(otelEndpoint string) (*grpc.ClientConn, error) {
	// It connects the OpenTelemetry Collector through local gRPC connection.
	conn, err := grpc.NewClient(otelEndpoint,
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel grpc connection to collector: %w", err)
	}

	return conn, nil
}

func newGrpcTraceExporter(ctx context.Context, conn *grpc.ClientConn) (trace.SpanExporter, error) {
	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create otel grpc trace exporter: %w", err)
	}
	return exporter, nil
}

func newFileTraceExporter(ctx context.Context, file *os.File) (trace.SpanExporter, error) {
	exporter, err := stdouttrace.New(
		// stdouttrace.WithPrettyPrint(),
		stdouttrace.WithWriter(file),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel file trace exporter: %w", err)
	}
	return exporter, nil
}

func newTraceProvider(traceExporter trace.SpanExporter, res *resource.Resource, spanNameFilter string, spanMinDurationFilter time.Duration) (*trace.TracerProvider, error) {
	var sp sdktrace.SpanProcessor
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	sp = bsp

	if spanNameFilter != "" {
		allowedPattern, err := regexp.Compile(spanNameFilter)
		if err != nil {
			return nil, fmt.Errorf("invalid span filter regex %s: %w", spanNameFilter, err)
		}
		filter := NameFilter{
			Next:           bsp,
			AllowedPattern: allowedPattern,
		}
		sp = filter
	}

	if spanMinDurationFilter > 0 {
		filter := DurationFilter{
			Next: sp,
			Min:  spanMinDurationFilter,
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
		return nil, fmt.Errorf("failed to create otel grpc metric exporter: %w", err)
	}
	return metricExporter, nil
}

func newFileMetricExporter(ctx context.Context, file *os.File) (sdkmetric.Exporter, error) {
	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithWriter(file),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create otel file metric exporter: %w", err)
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
