package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

// Init holds all information needed to initialize a server.
type Init struct {
	Address string
	Cluster []string
}

var initialFilenames = []string{
	"init.yaml",
	"cluster.key",
	"cluster.crt",
	"failure-domain",
}

// Load init.yaml if present, or return nil otherwise.
func loadInit(dir string) (*Init, error) {
	path := filepath.Join(dir, "init.yaml")

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "check if init.yaml exists")
		}
		return nil, nil

	}

	// Check that the only files in the directory are the TLS certificate.
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrap(err, "list data directory")
	}
	for _, file := range files {
		expected := false
		for _, filename := range initialFilenames {
			if filename == file.Name() {
				expected = true
				break
			}
		}
		if !expected {
			return nil, fmt.Errorf("data directory seems to have existing state: %s", file.Name())
		}
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "read init.yaml")
	}

	init := &Init{}
	if err := yaml.Unmarshal(data, init); err != nil {
		return nil, errors.Wrap(err, "parse init.yaml")
	}

	if init.Address == "" {
		return nil, fmt.Errorf("server address is empty")
	}

	return init, nil

}
