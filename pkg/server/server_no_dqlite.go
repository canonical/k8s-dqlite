//go:build !dqlite

package server

import (
	"context"
	"errors"
	"time"
)

var errNoDqlite = errors.New("dqlite is not supported, compile with \"-tags dqlite\"")
var closedCh = make(chan struct{})

func init() {
	close(closedCh)
}

type Server struct{}

func (*Server) Start(ctx context.Context) error    { return errNoDqlite }
func (*Server) MustStop() <-chan struct{}          { return closedCh }
func (*Server) Shutdown(ctx context.Context) error { return errNoDqlite }

func New(
	dir string,
	listen string,
	enableTLS bool,
	diskMode bool,
	clientSessionCacheSize uint,
	minTLSVersion string,
	watchAvailableStorageInterval time.Duration,
	watchAvailableStorageMinBytes uint64,
	lowAvailableStorageAction string,
	admissionControlPolicy string,
	admissionControlPolicyLimitMaxConcurrentTxn int64,
	admissionControlOnlyWriteQueries bool,
) (*Server, error) {
	return nil, errNoDqlite
}
