GO_SOURCES = $(shell find . -name '*.go')

## Development
go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	./hack/static-go-vet.sh ./...

go.test:
	go test -tags=libsqlite3 -v ./test

go.test.dqlite:
	./hack/static-go-test.sh -v ./test

go.bench:
	go test -tags=libsqlite3 -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.bench.dqlite:
	./hack/static-go-test.sh -v ./test -run "^$$" -bench "Benchmark" -benchmem

## Static Builds
static: bin/static/k8s-dqlite bin/static/dqlite

deps/static/lib/libdqlite.a:
	./hack/static-dqlite.sh

bin/static/k8s-dqlite: deps/static/lib/libdqlite.a $(GO_SOURCES)
	mkdir -p bin/static
	./hack/static-go-build.sh -o bin/static/k8s-dqlite ./k8s-dqlite.go

bin/static/dqlite: deps/static/lib/libdqlite.a
	mkdir -p bin/static
	rm -rf hack/.build/go-dqlite
	git clone https://github.com/canonical/go-dqlite --depth 1 -b v1.20.0 hack/.build/go-dqlite
	cd hack/.build/go-dqlite && ../../../hack/static-go-build.sh -o ../../../bin/static/dqlite ./cmd/dqlite

## Dynamic Builds
dynamic: bin/dynamic/k8s-dqlite bin/dynamic/dqlite

bin/dynamic/libdqlite.so:
	mkdir -p bin/dynamic
	./hack/dynamic-dqlite.sh
	echo here
	cp -rv ./hack/.deps/dynamic/lib/*.so* ./bin/dynamic/

bin/dynamic/k8s-dqlite: bin/dynamic/libdqlite.so $(GO_SOURCES)
	mkdir -p bin/dynamic
	./hack/dynamic-go-build.sh -o bin/dynamic/k8s-dqlite ./k8s-dqlite.go

bin/dynamic/dqlite: bin/dynamic/libdqlite.so
	mkdir -p bin/dynamic
	rm -rf hack/.build/go-dqlite
	git clone https://github.com/canonical/go-dqlite --depth 1 -b v1.20.0 hack/.build/go-dqlite
	cd hack/.build/go-dqlite && ../../../hack/dynamic-go-build.sh -o ../../../bin/dynamic/dqlite ./cmd/dqlite

## Cleanup
clean:
	rm -rf bin hack/.build hack/.deps
