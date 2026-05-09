// Package tracing wires OpenTelemetry for the processor.
//
// Init returns a shutdown function that flushes pending spans. When
// the OTEL_EXPORTER_OTLP_ENDPOINT env var is empty the function is a
// no-op: the global TracerProvider stays the default (noop) one, so
// the rest of the codebase can call `otel.Tracer(...).Start(...)`
// unconditionally without paying any cost outside of a traced run.
//
// Propagation uses W3C TraceContext + Baggage so that any future
// upstream/downstream service (or sidecar) interoperates without
// configuration tweaks.
package tracing

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Shutdown flushes any pending spans. Always safe to call.
type Shutdown func(context.Context) error

// Init configures the global TracerProvider and propagator.
// serviceName must match what shows up in the trace UI.
//
// Env vars honored:
//   - OTEL_EXPORTER_OTLP_ENDPOINT (e.g. http://jaeger:4318) — when empty,
//     this function is a no-op and tracing stays disabled.
//   - OTEL_EXPORTER_OTLP_INSECURE=true — disables TLS to the collector.
func Init(ctx context.Context, serviceName, serviceVersion string) (Shutdown, error) {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		// W3C propagator is still set so that if a future deploy enables
		// tracing on the *other* end of the pipeline, our service still
		// forwards the headers it receives.
		otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		))
		return func(context.Context) error { return nil }, nil
	}

	opts := []otlptracehttp.Option{otlptracehttp.WithEndpointURL(endpoint)}
	if os.Getenv("OTEL_EXPORTER_OTLP_INSECURE") == "true" {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	exporter, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("otlp trace exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		// Sample everything: this is a demo pipeline. In production
		// switch to ParentBased(TraceIDRatioBased(0.1)) or similar.
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}
