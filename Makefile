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

deps/lib/libdqlite.a: hack/compile-static-dqlite.sh
	./hack/compile-static-dqlite.sh

go.build.static: deps/lib/libdqlite.a
	env \
		PATH="${PATH}:${PWD}/deps/musl/bin" \
		CGO_CFLAGS="-I${PWD}/deps/include" \
		CGO_LDFLAGS="-L${PWD}/deps/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-tags=dqlite,libsqlite3 \
			-o k8s-dqlite \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			k8s-dqlite.go

dqlite: deps/lib/libdqlite.a
	go get github.com/canonical/go-dqlite/cmd/dqlite@v1.11.8
	env \
		PATH="${PATH}:${PWD}/deps/musl/bin" \
		CGO_CFLAGS="-I${PWD}/deps/include" \
		CGO_LDFLAGS="-L${PWD}/deps/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-tags=dqlite,libsqlite3 \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			-o dqlite \
			github.com/canonical/go-dqlite/cmd/dqlite
	go mod tidy

MIGRATOR_CFLAGS =
ifeq ($(shell uname -m), ppc64le)
	MIGRATOR_CFLAGS += -mlong-double-64
endif

migrator: deps/lib/libdqlite.a
	git clone https://github.com/canonical/go-migrator
	cd go-migrator && env \
		PATH="${PATH}:${PWD}/deps/musl/bin" \
		CGO_CFLAGS="$(MIGRATOR_CFLAGS) -I${PWD}/deps/include" \
		CGO_LDFLAGS="-L${PWD}/deps/lib -luv -lraft -ldqlite -llz4 -lsqlite3" \
		CGO_LDFLAGS_ALLOW="-Wl,-z,now" \
		CC="musl-gcc" \
		go build \
			-ldflags '-s -w -linkmode "external" -extldflags "-static"' \
			-o ../migrator \
			main.go
	cd ..
	rm -rf go-migrator
