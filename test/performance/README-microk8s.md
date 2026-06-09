# MicroK8s Integration Testing

This directory contains integration tests for k8s-dqlite using MicroK8s as the test platform.

## Overview

MicroK8s is the primary integration test target for k8s-dqlite since it continues to use k8s-dqlite as its datastore (unlike k8s-snap, which switched to etcd as the default in v1.35 and removed k8s-dqlite entirely in v1.36+).

## Test Strategy

### 1. Upstream MicroK8s Tests

We clone the canonical/microk8s repository and run their test suite with patched k8s-dqlite binaries. This ensures our changes work with MicroK8s's actual usage patterns.

**Tests executed:**
- `smoke-test.sh` - Basic cluster health checks
- `tox -e simple` - Integration tests for pods, services, ingress  
- `tox -e cluster` - Unit tests for join/leave operations with dqlite

**Patching approach:**
1. Install MicroK8s snap
2. Stop MicroK8s services (`snap stop microk8s`)
3. Replace `/snap/microk8s/current/bin/k8s-dqlite` and `/snap/microk8s/current/bin/dqlite` with our binaries
4. Start MicroK8s services (`snap start microk8s`)
5. Run tests

### 2. Performance Benchmarks

We run performance tests using the same infrastructure as before (test/performance/), but with MicroK8s as the backend instead of k8s-snap.

**Key changes:**
- New `microk8s_util.py` module handles MicroK8s-specific operations
- `conftest.py` detects `USE_MICROK8S=true` environment variable
- MicroK8s uses addons instead of bootstrap configs

**Performance tests:**
- Single-node benchmarks
- Multi-node benchmarks (requires LXD)
- Read-heavy workloads

## CI Workflow

`.github/workflows/microk8s-integration.yaml` defines the CI pipeline:

```yaml
jobs:
  prepare: Build k8s-dqlite binaries
  test-microk8s-upstream: Run MicroK8s tests with patched binaries
  test-ha: Multi-node HA testing (scheduled runs only)
  performance: Performance benchmarks (scheduled runs + self-hosted runners)
```

## Running Locally

### Upstream Tests

```bash
# Build k8s-dqlite
make static

# Install MicroK8s
sudo snap install microk8s --classic

# Patch binaries
sudo snap stop microk8s
sudo cp bin/static/k8s-dqlite /snap/microk8s/current/bin/k8s-dqlite
sudo cp bin/static/dqlite /snap/microk8s/current/bin/dqlite
sudo snap start microk8s

# Clone MicroK8s and run tests
git clone https://github.com/canonical/microk8s.git /tmp/microk8s
cd /tmp/microk8s
./tests/smoke-test.sh
sudo tox -e simple
sudo tox -e cluster
```

### Performance Tests

```bash
cd test/performance

# Set environment variable
export USE_MICROK8S=true

# Copy binaries to test directory
mkdir -p bin/static
cp ../../bin/static/k8s-dqlite bin/static/
cp ../../bin/static/dqlite bin/static/

# Run tests
tox -e single_node
```

## Future Work

- [ ] Multi-node HA testing with LXD containers
- [ ] Test MicroK8s clustering with multiple dqlite nodes
- [ ] Performance baseline comparison (k8s-snap vs MicroK8s)
- [ ] Test MicroK8s addons that interact with k8s-dqlite
