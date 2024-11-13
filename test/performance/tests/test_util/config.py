#
# Copyright 2024 Canonical, Ltd.
#
import os
from pathlib import Path

DIR = Path(__file__).absolute().parent

# The following defaults are used to define how long to wait for a condition to be met.
DEFAULT_WAIT_RETRIES = int(os.getenv("TEST_DEFAULT_WAIT_RETRIES") or 50)
DEFAULT_WAIT_DELAY_S = int(os.getenv("TEST_DEFAULT_WAIT_DELAY_S") or 10)

MANIFESTS_DIR = DIR / ".." / ".." / "templates"

# INSPECTION_REPORTS_DIR is the directory where inspection reports are stored.
# If empty, no reports are generated.
INSPECTION_REPORTS_DIR = os.getenv("TEST_INSPECTION_REPORTS_DIR")

# SKIP_CLEANUP can be used to prevent machines to be automatically destroyed
# after the tests complete.
SKIP_CLEANUP = (os.getenv("TEST_SKIP_CLEANUP") or "") == "1"

# SNAP is the path to the snap under test.
SNAP = os.getenv("TEST_SNAP") or ""

# SNAP_NAME is the name of the snap under test.
SNAP_NAME = os.getenv("TEST_SNAP_NAME") or "k8s"

# KUBE_BURNER_URL is the version of kube-burner to use.
KUBE_BURNER_URL = (
    os.getenv("TEST_KUBE_BURNER_URL")
    or "https://github.com/kube-burner/kube-burner/releases/download/v1.2/kube-burner-1.2-Linux-x86_64.tar.gz"
)

# FLAVOR is the flavour to use for running the performance tests.
FLAVOR = os.getenv("TEST_FLAVOR") or ""

# SUBSTRATE is the substrate to use for running the performance tests.
# Default 'lxd'.
SUBSTRATE = os.getenv("TEST_SUBSTRATE") or "lxd"

# LXD_IMAGE is the image to use for LXD containers.
LXD_IMAGE = os.getenv("TEST_LXD_IMAGE") or "ubuntu:22.04"

# LXD_PROFILE is the profile to use for LXD containers.
LXD_PROFILE = (
    os.getenv("TEST_LXD_PROFILE")
    or (DIR / ".." / ".." / "lxd-profile.yaml").read_text()
)

# LXD_PROFILE_NAME is the profile name to use for LXD containers.
LXD_PROFILE_NAME = os.getenv("TEST_LXD_PROFILE_NAME") or "k8s-performance"
