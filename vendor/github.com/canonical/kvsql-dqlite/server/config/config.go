package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Config holds the server configuraton loaded from disk.
type Config struct {
	KeyPair       tls.Certificate
	Pool          *x509.CertPool
	Init          *Init   // Initialization parameters, for new servers.
	Address       string  // Server address
	Update        *Update // Configuration updates
	FailureDomain uint64
}

// Load current the configuration from disk.
func Load(dir string) (*Config, error) {
	// Migrate the legacy node store.
	if err := migrateNodeStore(dir); err != nil {
		return nil, err
	}

	// Load the TLS certificates.
	crt := filepath.Join(dir, "cluster.crt")
	key := filepath.Join(dir, "cluster.key")

	keypair, err := tls.LoadX509KeyPair(crt, key)
	if err != nil {
		return nil, errors.Wrap(err, "load keypair")
	}

	data, err := ioutil.ReadFile(crt)
	if err != nil {
		return nil, errors.Wrap(err, "read certificate")
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("bad certificate")
	}

	// Check if we're initializing a new node (i.e. there's an init.yaml).
	init, err := loadInit(dir)
	if err != nil {
		return nil, err
	}

	var update *Update
	if init == nil {
		update, err = loadUpdate(dir)
		if err != nil {
			return nil, err
		}
	}

	domain, err := loadFailureDomain(dir)
	if err != nil {
		return nil, err
	}

	config := &Config{
		KeyPair:       keypair,
		Pool:          pool,
		Init:          init,
		Update:        update,
		FailureDomain: domain,
	}

	return config, nil
}

// Load failure-domain if present, or return 0 otherwise.
func loadFailureDomain(dir string) (uint64, error) {
	path := filepath.Join(dir, "failure-domain")

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return 0, errors.Wrap(err, "check if failure-domain exists")
		}
		return 0, nil

	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return 0, errors.Wrap(err, "read failure-domain")
	}

	text := strings.Trim(string(data), "\n")

	n, err := strconv.Atoi(text)
	if err != nil || n < 0 {
		return 0, errors.Wrapf(err, "invalid failure domain %q", text)
	}

	return uint64(n), nil
}
