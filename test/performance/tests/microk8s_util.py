#
# Copyright 2026 Canonical, Ltd.
#
"""MicroK8s-specific utility functions for performance testing."""

import json
import logging
from typing import Any, List, Optional

from test_util import config, harness, util

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
    """Get the kubeconfig from a MicroK8s instance."""
    result = instance.exec(["microk8s", "config"])
    return result.stdout.decode("utf-8")


def get_join_token(primary: harness.Instance, _joining_node: harness.Instance) -> str:
    """Generate a single-use join token on the primary node.

    Each call to ``microk8s add-node`` produces a unique token, so calling
    this twice for different joining nodes naturally yields distinct tokens.
    The ``_joining_node`` argument is accepted for API compatibility with
    ``test_util.util.get_join_token`` but is not used.
    """
    out = primary.exec(["microk8s", "add-node"], capture_output=True)
    for line in out.stdout.decode().splitlines():
        line = line.strip()
        if line.startswith("microk8s join ") and "--worker" not in line:
            return line.split("microk8s join ", 1)[1].strip()
    raise harness.HarnessError(
        "Failed to parse join token from microk8s add-node output:\n"
        + out.stdout.decode()
    )


def join_cluster(instance: harness.Instance, join_token: str):
    """Join an existing MicroK8s cluster. Nodes join as control-plane by default."""
    instance.exec(["microk8s", "join", join_token])


def wait_until_ready(
    control_node: harness.Instance,
    instances: List[harness.Instance],
    retries: int = config.DEFAULT_WAIT_RETRIES,
    delay_s: int = config.DEFAULT_WAIT_DELAY_S,
):
    """Wait until all instances appear as Ready nodes in the MicroK8s cluster."""
    for instance in instances:
        node_name = util.hostname(instance)
        util.stubbornly(retries=retries, delay_s=delay_s).on(control_node).until(
            lambda p: " Ready" in p.stdout.decode()
        ).exec(["microk8s", "kubectl", "get", "node", node_name, "--no-headers"])
    LOG.info("All MicroK8s nodes ready")


def ready_nodes(control_node: harness.Instance) -> List[Any]:
    """Return the list of nodes in Ready state."""
    result = control_node.exec(
        ["microk8s", "kubectl", "get", "nodes", "-o", "json"], capture_output=True
    )
    node_list = json.loads(result.stdout.decode())
    return [
        node
        for node in node_list["items"]
        if all(
            condition["status"] == "False"
            for condition in node["status"]["conditions"]
            if condition["type"] != "Ready"
        )
    ]


def get_local_node_status(instance: harness.Instance) -> str:
    """Return a string describing the node role (mirrors util.get_local_node_status).

    All nodes joined via ``microk8s join`` without ``--worker`` are control-plane
    members, so this always returns a string containing "control-plane".
    """
    node_name = util.hostname(instance)
    result = instance.exec(
        ["microk8s", "kubectl", "get", "node", node_name, "--show-labels"],
        capture_output=True,
    )
    labels = result.stdout.decode()
    if "node-role.kubernetes.io/control-plane" in labels:
        return "control-plane"
    return "worker"
