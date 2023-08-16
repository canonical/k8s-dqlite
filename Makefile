go.fmt:
	go mod tidy
	go fmt ./...

go.vet:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go vet ./...

go.test:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -v ./test

go.test.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -v ./test

go.bench:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.bench.dqlite:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go test -tags=dqlite -v ./test -run "^$$" -bench "Benchmark" -benchmem

go.build:
	CGO_LDFLAGS_ALLOW=-Wl,-z,now go build -tags=libsqlite3 -o k8s-dqlite -ldflags '-s -w' k8s-dqlite.go
