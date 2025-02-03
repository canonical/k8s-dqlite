package go_dqlite

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/canonical/go-dqlite/v2/app"
	"github.com/canonical/go-dqlite/v2/client"
	"github.com/sirupsen/logrus"
)

// TODO dynamically retrieve
var (
	crt      = "/var/snap/k8s/common/var/lib/k8s-dqlite/cluster.crt"
	key      = "/var/snap/k8s/common/var/lib/k8s-dqlite/cluster.key"
	filePath = "file:///var/snap/k8s/common/var/lib/k8s-dqlite/cluster.yaml"
)

func getLeader(ctx context.Context, crt string, key string, filePath string) (string, error) {

	path := filePath[len("file://"):]
	if _, err := os.Stat(path); err != nil {
		return "", err
	}
	store, err := client.DefaultNodeStore(path)
	if err != nil {
		return "", err
	}

	dial := client.DefaultDialFunc
	cert, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return "", err
	}

	data, err := os.ReadFile(crt)
	if err != nil {
		return "", err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return "", fmt.Errorf("bad certificate")
	}

	config := app.SimpleDialTLSConfig(cert, pool)

	dial = client.DialFuncWithTLS(dial, config)

	cli, err := client.FindLeader(ctx, store, client.WithDialFunc(dial))
	if err != nil {
		return "", err
	}

	leader, err := cli.Leader(ctx)
	if err != nil {
		return "", err
	}
	if leader == nil {
		return "", nil
	}
	logrus.WithFields(logrus.Fields{"leader": leader.Address}).Debugf("Retrieved current leader.")

	return leader.Address, nil
}

func IsLeader(ctx context.Context) (bool, error) {
	nodeIP := "192.168.101.118" // TODO get me properly

	leader, err := getLeader(ctx, crt, key, filePath)
	if err != nil {
		return false, err
	}

	leaderIP, _, err := net.SplitHostPort(leader)
	if err != nil {
		return false, err
	}

	if leaderIP == nodeIP {
		return true, nil
	}

	return false, nil
}
