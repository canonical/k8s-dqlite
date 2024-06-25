DQLITE_BUILD_SCRIPTS_DIR ?= $(shell pwd)/hack
GO_SOURCES = $(shell find . -name '*.go')

## Development
go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-vet.sh ./...

go.test:
	go test -v -p 1 ./...

go.test.dqlite:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-test.sh -v ./...

go.bench:
	go test -tags=libsqlite3 -v ./... -run "^$$" -bench "Benchmark" -benchmem

go.bench.dqlite:
	$(DQLITE_BUILD_SCRIPTS_DIR)/static-go-test.sh -v ./... -run "^$$" -bench "Benchmark" -benchmem

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

## Cleanup
clean:
	rm -rf bin hack/.build hack/.deps
