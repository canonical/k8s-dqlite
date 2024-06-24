package etcd

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

type registerConfig struct {
	ClientURLs    []string `yaml:"client-urls,omitempty"`
	PeerURL       string   `yaml:"peer-url,omitempty"`
	TrustedCAFile string   `yaml:"trusted-ca-file,omitempty"`
	CertFile      string   `yaml:"cert-file,omitempty"`
	KeyFile       string   `yaml:"key-file,omitempty"`
}

func fileUnmarshal(v interface{}, path ...string) error {
	b, err := os.ReadFile(filepath.Join(path...))
	if err != nil {
		return fmt.Errorf("failed to read file contents: %w", err)
	}
	if err := yaml.Unmarshal(b, v); err != nil {
		return fmt.Errorf("failed to parse as yaml: %w", err)
	}
	return nil
}
