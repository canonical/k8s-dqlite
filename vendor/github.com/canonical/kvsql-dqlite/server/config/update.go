package config

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

// Update holds configuration updates.
type Update struct {
	Address string
}

// Load update.yaml if present, or return nil otherwise.
func loadUpdate(dir string) (*Update, error) {
	path := filepath.Join(dir, "update.yaml")

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "check if update.yaml exists")
		}
		return nil, nil

	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errors.Wrap(err, "read init.yaml")
	}

	update := &Update{}
	if err := yaml.Unmarshal(data, update); err != nil {
		return nil, errors.Wrap(err, "parse update.yaml")
	}

	if update.Address == "" {
		return nil, fmt.Errorf("server address is empty")
	}

	return update, nil
}
