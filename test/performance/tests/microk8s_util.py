#
# Copyright 2026 Canonical, Ltd.
#
"""MicroK8s-specific utility functions for performance testing."""

import logging
from pathlib import Path
from typing import Optional

from test_util import harness

LOG = logging.getLogger(__name__)


def setup_microk8s_snap(
    instance: harness.Instance, tmp_path: Path, channel: Optional[str] = None
):
    """Install and set up MicroK8s snap on an instance.

    Args:
        instance: The harness instance to set up.
        tmp_path: Temporary path for artifacts.
        channel: MicroK8s snap channel (default: latest/stable).
    """
    if channel is None:
        channel = "latest/stable"

    LOG.info(f"Installing MicroK8s from channel {channel}")

    # Install MicroK8s snap
    instance.exec(["snap", "install", "microk8s", "--classic", f"--channel={channel}"])

    # Wait for MicroK8s to be ready
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])

    LOG.info("MicroK8s installed and ready")


def patch_k8s_dqlite(instance: harness.Instance, tmp_path: Path):
    """Patch k8s-dqlite binaries in MicroK8s with locally built versions.

    Args:
        instance: The harness instance to patch.
        tmp_path: Temporary path containing the k8s-dqlite binaries.
    """
    LOG.info("Patching k8s-dqlite binaries in MicroK8s")

    # Path to local binaries (built in CI)
    local_dqlite = tmp_path / "bin" / "static" / "k8s-dqlite"
    local_dqlite_bin = tmp_path / "bin" / "static" / "dqlite"

    if not local_dqlite.exists():
        LOG.warning(f"k8s-dqlite binary not found at {local_dqlite}, skipping patch")
        return

    # Stop MicroK8s services
    instance.exec(["snap", "stop", "microk8s"])

    # Upload and replace binaries
    instance.send_file(str(local_dqlite), "/tmp/k8s-dqlite")
    instance.send_file(str(local_dqlite_bin), "/tmp/dqlite")

    instance.exec(["chmod", "+x", "/tmp/k8s-dqlite", "/tmp/dqlite"])
    instance.exec(["cp", "/tmp/k8s-dqlite", "/snap/microk8s/current/bin/k8s-dqlite"])
    instance.exec(["cp", "/tmp/dqlite", "/snap/microk8s/current/bin/dqlite"])

    # Start MicroK8s
    instance.exec(["snap", "start", "microk8s"])

    # Wait for services to be ready
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])

    LOG.info("k8s-dqlite binaries patched successfully")


def bootstrap_microk8s(instance: harness.Instance, addons: Optional[list[str]] = None):
    """Bootstrap MicroK8s with specified addons.

    Args:
        instance: The harness instance to bootstrap.
        addons: List of MicroK8s addons to enable.
    """
    if addons is None:
        addons = ["dns", "storage", "ingress"]

    LOG.info(f"Enabling MicroK8s addons: {addons}")

    # Enable each addon
    for addon in addons:
        instance.exec(["microk8s", "enable", addon])

    # Wait for cluster to be ready
    instance.exec(
        [
            "microk8s",
            "kubectl",
            "wait",
            "--for=condition=ready",
            "node",
            "--all",
            "--timeout=5m",
        ]
    )

    LOG.info("MicroK8s bootstrapped successfully")


def get_kubeconfig(instance: harness.Instance) -> str:
    """Get the kubeconfig from a MicroK8s instance.

    Args:
        instance: The harness instance.

    Returns:
        The kubeconfig as a string.
    """
    result = instance.exec(["microk8s", "config"])
    return result.stdout.decode("utf-8")
