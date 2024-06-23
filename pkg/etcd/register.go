package etcd

import (
	"context"
	"fmt"
	"os"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *etcd) hasValidSentinelFile() (bool, error) {
	b, err := os.ReadFile(e.sentinelFile)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return strings.TrimSpace(string(b)) == e.peerURL, nil
}

func (e *etcd) createSentinel() error {
	return os.WriteFile(e.sentinelFile, []byte(e.peerURL), 0600)
}

func (e *etcd) ensurePeerInCluster(ctx context.Context) (string, error) {
	if e.config.ClusterState == "new" {
		return "", nil
	}

	if hasSentinel, err := e.hasValidSentinelFile(); err != nil {
		return "", fmt.Errorf("failed to check sentinel file: %w", err)
	} else if hasSentinel {
		return "", nil
	}

	client, err := clientv3.New(e.clientConfig)
	if err != nil {
		return "", fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer client.Close()

	resp, err := client.Cluster.MemberList(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list cluster members: %w", err)
	}

	for _, member := range resp.Members {
		for _, url := range member.GetPeerURLs() {
			if e.peerURL == url {
				return "", nil
			}
		}
	}

	members, err := client.Cluster.MemberAdd(ctx, []string{e.peerURL})
	if err != nil {
		return "", fmt.Errorf("failed to add cluster member with peerURL %q: %w", e.peerURL, err)
	}

	defer func() {
		if err := e.createSentinel(); err != nil {
			panic(fmt.Sprintf("failed to create sentinel file: %v", err.Error()))
		}
	}()

	initialClusterParts := make([]string, 0, len(members.Members))
	for _, member := range members.Members {
		name := member.GetName()
		for _, url := range member.GetPeerURLs() {
			if url == e.peerURL {
				name = e.config.Name
			}
			initialClusterParts = append(initialClusterParts, fmt.Sprintf("%s=%s", name, url))
			break
		}
	}

	return strings.Join(initialClusterParts, ","), nil
}
