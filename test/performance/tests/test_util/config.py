#
# Copyright 2025 Canonical, Ltd.
#
import os
from pathlib import Path

DIR = Path(__file__).absolute().parent

# The following defaults are used to define how long to wait for a condition to be met.
DEFAULT_WAIT_RETRIES = int(os.getenv("TEST_DEFAULT_WAIT_RETRIES") or 50)
DEFAULT_WAIT_DELAY_S = int(os.getenv("TEST_DEFAULT_WAIT_DELAY_S") or 10)

MANIFESTS_DIR = DIR / ".." / ".." / "templates"

METRICS_DIR = os.getenv("TEST_METRICS_DIR") or DIR / ".." / ".." / "results"

RUN_NAME = os.getenv("TEST_RUN_NAME") or "k8s"

# INSPECTION_REPORTS_DIR is the directory where inspection reports are stored.
# If empty, no reports are generated.
INSPECTION_REPORTS_DIR = os.getenv("TEST_INSPECTION_REPORTS_DIR")

# SKIP_CLEANUP can be used to prevent machines to be automatically destroyed
# after the tests complete.
SKIP_CLEANUP = (os.getenv("TEST_SKIP_CLEANUP") or "") == "1"

# Note that when using containers, this will override the host configuration.
CORE_DUMP_PATTERN = (os.getenv("TEST_CORE_DUMP_PATTERN")) or r"core-%e.%p.%h"
CORE_DUMP_DIR = (os.getenv("TEST_CORE_DUMP_DIR")) or "/var/crash"

# SNAP is the path to the snap under test.
SNAP = os.getenv("TEST_SNAP") or ""

# SNAP_NAME is the name of the snap under test.
SNAP_NAME = os.getenv("TEST_SNAP_NAME") or "k8s"

# KUBE_BURNER_URL is the version of kube-burner to use.
KUBE_BURNER_URL = (
    os.getenv("TEST_KUBE_BURNER_URL")
    or "https://github.com/kube-burner/kube-burner/releases/download/v1.2/kube-burner-1.2-Linux-x86_64.tar.gz"
)

# Global kube-burner invocation timeout.
KUBE_BURNER_TIMEOUT = os.getenv("TEST_KUBE_BURNER_TIMEOUT") or "10m"

# FLAVOR is the flavour to use for running the performance tests.
FLAVOR = os.getenv("TEST_FLAVOR") or ""

# SUBSTRATE is the substrate to use for running the performance tests.
# Default 'lxd'.
SUBSTRATE = os.getenv("TEST_SUBSTRATE") or "lxd"

# LXD_IMAGE is the image to use for LXD containers.
LXD_IMAGE = os.getenv("TEST_LXD_IMAGE") or "ubuntu:22.04"

# LXD_PROFILE_NAME is the profile name to use for LXD containers.
LXD_PROFILE_NAME = os.getenv("TEST_LXD_PROFILE_NAME") or "k8s-performance"

# Enable k8s-dqlite debug logging.
K8S_DQLITE_DEBUG = os.getenv("TEST_K8S_DQLITE_DEBUG") == "1"
# Set the following to 1 for verbose dqlite trace messages.
DQLITE_TRACE_LEVEL = os.getenv("TEST_DQLITE_TRACE_LEVEL")
RAFT_TRACE_LEVEL = os.getenv("TEST_RAFT_TRACE_LEVEL")

# Enable pprof profiling.
ENABLE_PROFILING = os.getenv("TEST_ENABLE_PROFILING", "1") == "1"
