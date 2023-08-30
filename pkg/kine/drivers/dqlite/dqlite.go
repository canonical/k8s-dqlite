//go:build dqlite
// +build dqlite

package dqlite

import (
	"context"
	crypto_tls "crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/go-dqlite/driver"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	Dialer = client.DefaultDialFunc
	Logger = client.DefaultLogFunc
)

func init() {
	// We assume SQLite will be used multi-threaded
	if err := dqlite.ConfigMultiThread(); err != nil {
		panic(errors.Wrap(err, "failed to set dqlite multithreaded mode"))
	}
}

type opts struct {
	peers      []client.NodeInfo
	peerFile   string
	dsn        string
	driverName string // If not empty, use a pre-registered dqlite driver

	compactInterval time.Duration
	pollInterval    time.Duration
}

func AddPeers(ctx context.Context, nodeStore client.NodeStore, additionalPeers ...client.NodeInfo) error {
	existing, err := nodeStore.Get(ctx)
	if err != nil {
		return err
	}

	var peers []client.NodeInfo

outer:
	for _, peer := range additionalPeers {
		for _, check := range existing {
			if check.Address == peer.Address {
				continue outer
			}
		}
		peers = append(peers, peer)
	}

	if len(peers) > 0 {
		err = nodeStore.Set(ctx, append(existing, peers...))
		if err != nil {
			return err
		}
	}

	return nil
}

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config) (server.Backend, error) {
	logrus.Printf("New kine for dqlite.")
	opts, err := parseOpts(datasourceName)
	if err != nil {
		return nil, err
	}

	var nodeStore client.NodeStore
	if opts.peerFile != "" {
		nodeStore, err = client.DefaultNodeStore(opts.peerFile)
		if err != nil {
			return nil, errors.Wrap(err, "opening peerfile")
		}
	} else {
		nodeStore = client.NewInmemNodeStore()
	}

	if err := AddPeers(ctx, nodeStore, opts.peers...); err != nil {
		return nil, errors.Wrap(err, "add peers")
	}

	logrus.Printf("DriverName is %s.", opts.driverName)
	if opts.driverName == "" {
		opts.driverName = "dqlite"
		dial, err := getDialer(tlsInfo)
		if err != nil {
			return nil, err
		}
		d, err := driver.New(nodeStore,
			driver.WithLogFunc(Logger),
			driver.WithContext(ctx),
			dial)
		if err != nil {
			return nil, errors.Wrap(err, "new dqlite driver")
		}
		sql.Register(opts.driverName, d)
	}

	backend, generic, err := sqlite.NewVariant(ctx, opts.driverName, opts.dsn)
	if err != nil {
		return nil, errors.Wrap(err, "sqlite client")
	}
	if err := migrate(ctx, generic.DB); err != nil {
		return nil, errors.Wrap(err, "failed to migrate DB from sqlite")
	}

	generic.LockWrites = true
	generic.Retry = func(err error) bool {
		// get the inner-most error if possible
		err = errors.Cause(err)

		if err, ok := err.(driver.Error); ok {
			return err.Code == driver.ErrBusy
		}

		if err == sqlite3.ErrLocked || err == sqlite3.ErrBusy {
			return true
		}

		if strings.Contains(err.Error(), "database is locked") {
			return true
		}

		if strings.Contains(err.Error(), "cannot start a transaction within a transaction") {
			return true
		}

		if strings.Contains(err.Error(), "bad connection") {
			return true
		}

		if strings.Contains(err.Error(), "checkpoint in progress") {
			return true
		}

		return false
	}
	generic.TranslateErr = func(err error) error {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return server.ErrKeyExists
		}
		return err
	}

	generic.CompactInterval = opts.compactInterval
	generic.PollInterval = opts.pollInterval
	return backend, nil
}

func getDialer(tlsInfo tls.Config) (driver.Option, error) {
	dial := client.DefaultDialFunc
	if (tlsInfo.CertFile != "" && tlsInfo.KeyFile == "") || (tlsInfo.KeyFile != "" && tlsInfo.CertFile == "") {
		return nil, errors.New("both TLS certificate and key must be given")
	}
	if tlsInfo.CertFile != "" {
		cert, err := crypto_tls.LoadX509KeyPair(tlsInfo.CertFile, tlsInfo.KeyFile)
		if err != nil {
			return nil, errors.New("bad certificate pair")
		}

		data, err := ioutil.ReadFile(tlsInfo.CertFile)
		if err != nil {
			return nil, errors.New("could not read certificate")
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(data) {
			return nil, errors.New("bad certificate")
		}

		config := app.SimpleDialTLSConfig(cert, pool)
		dial = client.DialFuncWithTLS(dial, config)
		logrus.Printf("dial set to DialFuncWithTLS")
	}
	return driver.WithDialFunc(dial), nil
}

func migrate(ctx context.Context, newDB *sql.DB) (exitErr error) {
	row := newDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM kine")
	var count int64
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	if _, err := os.Stat("./db/state.db"); err != nil {
		return nil
	}

	oldDB, err := sql.Open("sqlite3", "./db/state.db")
	if err != nil {
		return nil
	}
	defer oldDB.Close()

	oldData, err := oldDB.QueryContext(ctx, "SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value FROM kine")
	if err != nil {
		logrus.Errorf("failed to find old data to migrate: %v", err)
		return nil
	}
	defer oldData.Close()

	tx, err := newDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if exitErr == nil {
			exitErr = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	for oldData.Next() {
		row := []interface{}{
			new(int),
			new(string),
			new(int),
			new(int),
			new(int),
			new(int),
			new(int),
			new([]byte),
			new([]byte),
		}
		if err := oldData.Scan(row...); err != nil {
			return err
		}

		if _, err := newDB.ExecContext(ctx, "INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value) values(?, ?, ?, ?, ?, ?, ?, ?, ?)",
			row...); err != nil {
			return err
		}
	}

	if err := oldData.Err(); err != nil {
		return err
	}

	return nil
}

func parseOpts(dsn string) (opts, error) {
	result := opts{
		dsn: dsn,
	}

	parts := strings.SplitN(dsn, "?", 2)
	if len(parts) == 1 {
		return result, nil
	}

	values, err := url.ParseQuery(parts[1])
	if err != nil {
		return result, err
	}

	for k, vs := range values {
		if len(vs) == 0 {
			continue
		}

		switch k {
		case "peer":
			for _, v := range vs {
				parts := strings.SplitN(v, ":", 3)
				if len(parts) != 3 {
					return result, fmt.Errorf("must be ID:IP:PORT format got: %s", v)
				}
				id, err := strconv.ParseUint(parts[0], 10, 64)
				if err != nil {
					return result, errors.Wrapf(err, "failed to parse %s", parts[0])
				}
				result.peers = append(result.peers, client.NodeInfo{
					ID:      id,
					Address: parts[1] + ":" + parts[2],
				})
			}
			delete(values, k)
		case "peer-file":
			result.peerFile = vs[0]
			delete(values, k)
		case "driver-name":
			result.driverName = vs[0]
			delete(values, k)
		case "compact-interval":
			d, err := time.ParseDuration(vs[0])
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse compact-interval duration value %q: %w", vs[0], err)
			}
			result.compactInterval = d
			delete(values, k)
		case "poll-interval":
			d, err := time.ParseDuration(vs[0])
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse poll-interval duration value %q: %w", vs[0], err)
			}
			result.pollInterval = d
			delete(values, k)
		}
	}

	if len(values) == 0 {
		result.dsn = parts[0]
	} else {
		result.dsn = fmt.Sprintf("%s?%s", parts[0], values.Encode())
	}

	return result, nil
}
