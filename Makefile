DQLITE_BUILD_SCRIPTS_DIR ?= $(shell pwd)/hack
GO_SOURCES = $(shell find . -name '*.go')
BENCH_COUNT ?= 7

## Development
go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-vet.sh ./...

go.test:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-test.sh -v -p 1 ./...

go.bench:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-test.sh -v -p 1 ./... -run "^$$" -bench "Benchmark" -benchmem -count $(BENCH_COUNT)

## Static Builds
static: bin/static/k8s-dqlite bin/static/dqlite

$(DQLITE_BUILD_SCRIPTS_DIR)/.deps/static/lib/libdqlite.a:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-dqlite.sh

bin/static/k8s-dqlite: $(DQLITE_BUILD_SCRIPTS_DIR)/.deps/static/lib/libdqlite.a $(GO_SOURCES)
	mkdir -p bin/static
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-build.sh -o bin/static/k8s-dqlite ./k8s-dqlite.go

bin/static/dqlite: $(DQLITE_BUILD_SCRIPTS_DIR)/.deps/static/lib/libdqlite.a
	mkdir -p bin/static
	GOBIN=$(shell pwd)/bin/static $(DQLITE_BUILD_SCRIPTS_DIR)/static-go-install.sh github.com/canonical/go-dqlite/cmd/dqlite@v1.20.0

## Dynamic Builds
dynamic: bin/dynamic/k8s-dqlite bin/dynamic/dqlite

bin/dynamic/lib/libdqlite.so:
	mkdir -p bin/dynamic/lib
	$(DQLITE_BUILD_SCRIPTS_DIR)/dynamic-dqlite.sh
	cp -rv $(DQLITE_BUILD_SCRIPTS_DIR)/.deps/dynamic/lib/*.so* ./bin/dynamic/lib/

bin/dynamic/k8s-dqlite: bin/dynamic/lib/libdqlite.so $(GO_SOURCES)
	mkdir -p bin/dynamic
	$(DQLITE_BUILD_SCRIPTS_DIR)/dynamic-go-build.sh -o bin/dynamic/k8s-dqlite ./k8s-dqlite.go

bin/dynamic/dqlite: bin/dynamic/lib/libdqlite.so
	mkdir -p bin/dynamic
	GOBIN=$(shell pwd)/bin/dynamic $(DQLITE_BUILD_SCRIPTS_DIR)/dynamic-go-install.sh github.com/canonical/go-dqlite/cmd/dqlite@v1.20.0

# Tracing logs export to local jaeger instance
# 4317	HTTP (OTLP) over gRPC
# 4318	HTTP (OTLP) over HTTP
# connect http://localhost:16686
jaeger:
	docker run --name jaeger \
		-e COLLECTOR_OTLP_ENABLED=true \
		-p 16686:16686 \
		-p 4317:4317 \
		-p 4318:4318 \
		jaegertracing/all-in-one:1.35


## Cleanup
clean:
	rm -rf bin hack/.build hack/.deps
