package server

import "time"

// InitConfiguration is the configuration format for init.yaml
type InitConfiguration struct {
	// Address is the bind address to use for this node.
	Address string `yaml:"Address"`
	// Cluster is a list of "host:port" addresses of existing cluster nodes.
	Cluster []string `yaml:"Cluster"`
}

// UpdateConfiguration is the configuration format for update.yaml
type UpdateConfiguration struct {
	// Address is the new bind address to use for this node.
	Address string `yaml:"Address"`
}

// FailureDomainConfiguration is the configuration format for failure-domain (just an integer)
type FailureDomainConfiguration uint64

// TuningConfiguration is configuration for tuning dqlite and kine parameters
type TuningConfiguration struct {
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
