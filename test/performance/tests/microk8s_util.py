#
# Copyright 2026 Canonical, Ltd.
#
"""MicroK8s-specific utility functions for performance testing."""

import logging
from typing import Optional

from test_util import config, harness

LOG = logging.getLogger(__name__)

MICROK8S_DQLITE_ARGS_FILE = "/var/snap/microk8s/current/args/k8s-dqlite"


def setup_microk8s_snap(instance: harness.Instance, channel: Optional[str] = None):
    """Install and set up MicroK8s snap on an instance.

    Args:
        instance: The harness instance to set up.
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


def configure_microk8s_dqlite(instance: harness.Instance):
    """Configure k8s-dqlite args for MicroK8s (OTEL, profiling).

    Mirrors the logic in test_util.util.configure_dqlite but targets the
    MicroK8s args file path.  The service is restarted after to apply changes.
    """
    if config.DQLITE_TRACE_LEVEL:
        instance.exec(
            [
                "echo",
                f"LIBDQLITE_TRACE={config.DQLITE_TRACE_LEVEL}",
                ">>",
                "/var/snap/microk8s/current/args/k8s-dqlite-env",
            ]
        )
    if config.RAFT_TRACE_LEVEL:
        instance.exec(
            [
                "echo",
                f"LIBRAFT_TRACE={config.RAFT_TRACE_LEVEL}",
                ">>",
                "/var/snap/microk8s/current/args/k8s-dqlite-env",
            ]
        )
    if config.K8S_DQLITE_DEBUG:
        instance.exec(["echo", "--debug", ">>", MICROK8S_DQLITE_ARGS_FILE])

    if config.ENABLE_PROFILING:
        instance.exec(["echo", "--profiling", ">>", MICROK8S_DQLITE_ARGS_FILE])
        instance.exec(
            ["echo", "--profiling-dir=/root", ">>", MICROK8S_DQLITE_ARGS_FILE]
        )

    if config.OTEL_ENABLED:
        instance.exec(["echo", "--otel", ">>", MICROK8S_DQLITE_ARGS_FILE])
        instance.exec(["echo", "--otel-dir=/root", ">>", MICROK8S_DQLITE_ARGS_FILE])

        if config.OTEL_SPAN_NAME_FILTER:
            instance.exec(
                [
                    "echo",
                    f"--otel-span-name-filter='{config.OTEL_SPAN_NAME_FILTER}'",
                    ">>",
                    MICROK8S_DQLITE_ARGS_FILE,
                ]
            )

        if config.OTEL_SPAN_MIN_DURATION_FILTER:
            instance.exec(
                [
                    "echo",
                    f"--otel-span-min-duration-filter={config.OTEL_SPAN_MIN_DURATION_FILTER}",
                    ">>",
                    MICROK8S_DQLITE_ARGS_FILE,
                ]
            )

    # Restart only the dqlite daemon so changes take effect
    instance.exec(["snap", "restart", "microk8s.daemon-k8s-dqlite"])
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])


def patch_k8s_dqlite(instance: harness.Instance):
    """Patch k8s-dqlite binaries in MicroK8s with locally built versions.

    Uses config.K8S_DQLITE_BIN_DIR to locate the binaries.  After patching,
    configures k8s-dqlite args (OTEL, profiling) to match what k8s-snap
    integration tests expect.

    Args:
        instance: The harness instance to patch.
    """
    LOG.info("Patching k8s-dqlite binaries in MicroK8s")

    local_k8s_dqlite = config.K8S_DQLITE_BIN_DIR / "k8s-dqlite"
    local_dqlite = config.K8S_DQLITE_BIN_DIR / "dqlite"

    if not local_k8s_dqlite.exists():
        LOG.warning(
            "k8s-dqlite binary not found at %s, skipping patch", local_k8s_dqlite
        )
        return

    # Stop MicroK8s services
    instance.exec(["snap", "stop", "microk8s"])

    # Upload binaries to a writable location
    instance.send_file(str(local_k8s_dqlite), "/tmp/k8s-dqlite")
    instance.send_file(str(local_dqlite), "/tmp/dqlite")

    instance.exec(["chmod", "+x", "/tmp/k8s-dqlite", "/tmp/dqlite"])

    # /snap/microk8s/current/ is a read-only squashfs mount so we cannot
    # use cp.  Bind-mount the new binaries over the snap files instead.
    # The LXD profile is privileged + unconfined so bind mounts work.
    instance.exec(
        ["mount", "--bind", "/tmp/k8s-dqlite", "/snap/microk8s/current/bin/k8s-dqlite"]
    )
    instance.exec(
        ["mount", "--bind", "/tmp/dqlite", "/snap/microk8s/current/bin/dqlite"]
    )

    # Start MicroK8s
    instance.exec(["snap", "start", "microk8s"])

    # Wait for services to be ready
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])

    LOG.info("k8s-dqlite binaries patched successfully")

    # Configure k8s-dqlite args (OTEL, profiling) now that we know the binary
    # supports these features (it is our own build).
    configure_microk8s_dqlite(instance)


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
