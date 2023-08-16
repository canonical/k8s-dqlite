package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

type DqliteTuning struct {
	// Snapshot is tuning for the raft snapshot parameters.
	// If non-nil, it is set with app.WithSnapshotParams() when starting dqlite.
	Snapshot *struct {
		Threshold uint64 `yaml:"threshold"`
		Trailing  uint64 `yaml:"trailing"`
	} `yaml:"snapshot"`

	// NetworkLatency is the average one-way network latency between dqlite nodes.
	// If non-nil, it is passed as app.WithNetworkLatency() when starting dqlite.
	NetworkLatency *time.Duration `yaml:"network-latency"`

	// KineCompactInterval is the interval between kine database compaction operations.
	KineCompactInterval *time.Duration `yaml:"kine-compact-interval"`

	// KinePollInterval is the kine poll interval.
	KinePollInterval *time.Duration `yaml:"kine-poll-interval"`
}

func loadDqliteTuningParameters(dir string) (DqliteTuning, error) {
	var v DqliteTuning

	path := filepath.Join(dir, "tuning.yaml")
	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return v, nil
		}
		return v, fmt.Errorf("failed to stat dqlite tuning configuration: %w", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return v, fmt.Errorf("failed to read dqlite tuning configuration: %w", err)
	}

	if err := yaml.Unmarshal(b, &v); err != nil {
		return v, fmt.Errorf("failed to parse dqlite tuning configuration: %w", err)
	}

	return v, nil
}
