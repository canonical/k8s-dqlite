package server

import (
	"go.opentelemetry.io/otel"
)

const name = "limited-server"

var (
	tracer = otel.Tracer(name)
	meter  = otel.Meter(name)
)
