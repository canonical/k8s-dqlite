package limited

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

func (l *LimitedServer) DbSize(ctx context.Context) (int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.DbSize", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	size, err := l.driver.GetSize(ctx)
	span.SetAttributes(attribute.Int64("size", size))
	return size, err
}
