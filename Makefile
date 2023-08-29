## Development
go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go vet ./...

go.test:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -v ./test

go.test.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -tags=dqlite -v ./test

go.bench:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.bench.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -tags=dqlite -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.build:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go build -tags=dqlite,libsqlite3 -o k8s-dqlite -ldflags '-s -w' k8s-dqlite.go

## Compile configuration
MIGRATOR_CFLAGS =
ifeq ($(shell uname -m), ppc64le)
	MIGRATOR_CFLAGS += -mlong-double-64
endif

## Static Builds
static: bin/static/k8s-dqlite bin/static/dqlite bin/static/migrator

deps/static/lib/libdqlite.a: hack/compile-static-dqlite.sh
	./hack/compile-static-dqlite.sh

bin/static/k8s-dqlite: deps/static/lib/libdqlite.a
	mkdir -p bin/static
	env \
		PATH="${PATH}:${PWD}/deps/static/musl/bin" \
		CGO_CFLAGS="-I${PWD}/deps/static/include" \
		CGO_LDFLAGS="-L${PWD}/deps/static/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-tags=dqlite,libsqlite3 \
			-o bin/static/k8s-dqlite \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			k8s-dqlite.go

bin/static/dqlite: deps/static/lib/libdqlite.a
	mkdir -p bin/static
	rm -rf build/go-dqlite
	git clone https://github.com/canonical/go-dqlite --depth 1 -b v1.11.8 build/go-dqlite
	cd build/go-dqlite && env \
		PATH="${PATH}:${PWD}/deps/static/musl/bin" \
		CGO_CFLAGS="-I${PWD}/deps/static/include" \
		CGO_LDFLAGS="-L${PWD}/deps/static/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-tags=dqlite,libsqlite3 \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			-o ${PWD}/bin/static/dqlite \
			github.com/canonical/go-dqlite/cmd/dqlite


bin/static/migrator: deps/static/lib/libdqlite.a
	mkdir -p bin/static
	rm -rf build/go-migrator
	git clone https://github.com/canonical/go-migrator --depth 1 build/go-migrator
	cd build/go-migrator && env \
		PATH="${PATH}:${PWD}/deps/static/musl/bin" \
		CGO_CFLAGS="$(MIGRATOR_CFLAGS) -I${PWD}/deps/static/include" \
		CGO_LDFLAGS="-L${PWD}/deps/static/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			-o ${PWD}/bin/static/migrator \
			main.go

## Dynamic Builds
dynamic: bin/dynamic/lib/libdqlite.so bin/dynamic/k8s-dqlite bin/dynamic/dqlite bin/dynamic/migrator

bin/dynamic/lib/libdqlite.so: deps/dynamic/lib/libdqlite.so
	mkdir -p bin/dynamic/lib
	cp deps/dynamic/lib/*.so* bin/dynamic/lib

deps/dynamic/lib/libdqlite.so:
	./hack/compile-dynamic-dqlite.sh

bin/dynamic/k8s-dqlite: deps/dynamic/lib/libdqlite.so
	mkdir -p bin/dynamic
	env \
		CGO_CFLAGS="-I${PWD}/deps/dynamic/include" \
		CGO_LDFLAGS="-L${PWD}/deps/dynamic/lib -lraft -luv -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		go build \
			-tags=dqlite,libsqlite3 \
			-o bin/dynamic/k8s-dqlite \
			-ldflags '-s -w' \
			k8s-dqlite.go

bin/dynamic/dqlite: deps/dynamic/lib/libdqlite.so
	mkdir -p bin/dynamic
	rm -rf build/go-dqlite
	git clone https://github.com/canonical/go-dqlite --depth 1 -b v1.11.8 build/go-dqlite
	cd build/go-dqlite && env \
		CGO_CFLAGS="-I${PWD}/deps/dynamic/include" \
		CGO_LDFLAGS="-L${PWD}/deps/dynamic/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		go build \
			-tags=dqlite,libsqlite3 \
			-ldflags '-s -w' \
			-o ${PWD}/bin/dynamic/dqlite \
			./cmd/dqlite

bin/dynamic/migrator: deps/dynamic/lib/libdqlite.so
	mkdir -p bin/dynamic
	rm -rf build/go-migrator
	git clone https://github.com/canonical/go-migrator --depth 1 build/go-migrator
	cd build/go-migrator && env \
		CGO_CFLAGS="$(MIGRATOR_CFLAGS) -I${PWD}/deps/dynamic/include" \
		CGO_LDFLAGS="-L${PWD}/deps/dynamic/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		go build \
			-ldflags '-s -w' \
			-o ${PWD}/bin/dynamic/migrator \
			./main.go

## Cleanup
clean:
	rm -rf bin build deps
