package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// testWatchEventPollTimeout is the timeout for waiting to receive an event.
	testWatchEventPollTimeout = 50 * time.Millisecond

	// testWatchEventIdleTimeout is the amount of time to wait to ensure that no events
	// are received when they should not.
	testWatchEventIdleTimeout = 100 * time.Millisecond

	// testExpirePollPeriod is the polling period for waiting for lease expiration
	testExpirePollPeriod = 100 * time.Millisecond
)

// newKine spins up a new instance of kine.
//
// newKine will create a sqlite or dqlite endpoint based on the provided go build tags (see e.g. util_test_dqlite.go)
// Custom endpoint query parameters can be configured with the `qs` parameter (e.g. "admission-control-policy=limit")
//
// newKine will panic in case of error
//
// newKine will return a context as well as a configured etcd client for the kine instance
func newKine(ctx context.Context, tb testing.TB, qs ...string) (*clientv3.Client, server.Backend) {
	logrus.SetLevel(logrus.ErrorLevel)

	endpointConfig := makeEndpointConfig(ctx, tb)
	if !strings.Contains(endpointConfig.Endpoint, "?") {
		endpointConfig.Endpoint += "?"
	}
	for _, v := range qs {
		endpointConfig.Endpoint = fmt.Sprintf("%s&%s", endpointConfig.Endpoint, v)
	}
	config, backend, err := endpoint.ListenAndReturnBackend(ctx, endpointConfig)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		backend.Wait()
	})
	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		tb.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointConfig.Listener},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client, backend
}
