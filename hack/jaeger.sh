#!/bin/bash

# This script sets up a local jaeger instance for the otel tracing logs
# 4317	HTTP (OTLP) over gRPC
# 4318	HTTP (OTLP) over HTTP
# connect via http://localhost:16686
docker run --name jaeger \
    -e COLLECTOR_OTLP_ENABLED=true \
    -p 16686:16686 \
    -p 4317:4317 \
    -p 4318:4318 \
    jaegertracing/all-in-one:1.35

