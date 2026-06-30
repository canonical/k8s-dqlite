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


def setup_microk8s_snap(
    instance: harness.Instance,
    channel: Optional[str] = None,
    registry=None,
):
    """Install and set up MicroK8s snap on an instance.

    Args:
        instance: The harness instance to set up.
        channel: MicroK8s snap channel (default: latest/stable).
        registry: Optional Registry instance; if provided, containerd will be
            configured to pull images through the local mirror instead of
            reaching out to Docker Hub directly.
    """
    if channel is None:
        channel = "latest/stable"

    LOG.info(f"Installing MicroK8s from channel {channel}")

    instance.exec(["snap", "install", "microk8s", "--classic", f"--channel={channel}"])

    if registry is not None:
        registry.apply_to_microk8s(instance)

    # Wait for MicroK8s to be ready
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])

    LOG.info("MicroK8s installed and ready")


def _write_dqlite_args(instance: harness.Instance):
    """Write k8s-dqlite args to the MicroK8s args file.

    Must be called while MicroK8s is stopped so snapd is idle and the args
    take effect on the next start without requiring a separate daemon restart.
    """
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

    _write_dqlite_args(instance)

    # Restart only the dqlite daemon so changes take effect.
    # Use systemctl directly rather than ``snap restart`` to avoid creating a
    # snapd transaction: ``snap restart`` goes through the snapd API and creates
    # a "Running service command" snap change that leaves snapd in a busy state
    # for several minutes on fresh installs.  During that window any subsequent
    # ``lxc exec`` call that invokes a snap app (e.g. ``microk8s enable``) fails
    # with exit 243 (LXD websocket connection reset).
    instance.exec(
        ["systemctl", "restart", "snap.microk8s.daemon-k8s-dqlite"]
    )
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])


def patch_k8s_dqlite(instance: harness.Instance):
    """Patch k8s-dqlite binaries in MicroK8s with locally built versions.

    Bind-mounts the locally built binaries over the snap's read-only squashfs
    while MicroK8s is running, then restarts only the dqlite daemon.  This
    avoids a full ``snap stop/start`` cycle whose post-start snapd activity
    (certificate generation, calico setup, etc.) can cause subsequent
    ``lxc exec`` calls to fail with exit 243/141.

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

    # Upload binaries to a writable location (MicroK8s keeps running).
    instance.send_file(str(local_k8s_dqlite), "/tmp/k8s-dqlite")
    instance.send_file(str(local_dqlite), "/tmp/dqlite")
    instance.exec(["chmod", "+x", "/tmp/k8s-dqlite", "/tmp/dqlite"])

    # /snap/microk8s/current/ is a read-only squashfs mount.  Bind-mount the
    # new binaries over the snap files.  The LXD profile is privileged +
    # unconfined so bind mounts work even while the snap is running.
    instance.exec(
        ["mount", "--bind", "/tmp/k8s-dqlite", "/snap/microk8s/current/bin/k8s-dqlite"]
    )
    instance.exec(
        ["mount", "--bind", "/tmp/dqlite", "/snap/microk8s/current/bin/dqlite"]
    )

    # Write k8s-dqlite args before restarting the daemon.
    _write_dqlite_args(instance)

    # Restart only the dqlite daemon to pick up the new binary and args.
    # Use systemctl directly (not ``snap restart``) to avoid creating a snapd
    # transaction that puts snapd in a busy state and causes subsequent
    # ``microk8s enable`` calls to fail with LXD exit 243.
    instance.exec(
        ["systemctl", "restart", "snap.microk8s.daemon-k8s-dqlite"]
    )
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

    # Wait for MicroK8s to be fully ready before enabling addons.
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])

    # Enable all addons in a single invocation so MicroK8s handles sequencing.
    # The harness retries on transient LXD exit codes (141/243).
    instance.exec(["microk8s", "enable"] + addons)

    # Wait for all nodes to be ready after addon deployment.
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
    result = instance.exec(["microk8s", "config"], capture_output=True)
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
    # Wait for this node's services to be fully up before returning so that
    # wait_until_ready() can immediately query kubectl get nodes.
    instance.exec(["microk8s", "status", "--wait-ready", "--timeout=300"])


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
    """Return a string describing the node role.

    In MicroK8s HA, nodes that joined without --worker run the dqlite daemon
    and are control-plane members.  Check the service state rather than
    Kubernetes labels (which MicroK8s does not set for control-plane nodes).
    """
    result = instance.exec(
        ["systemctl", "is-active", "snap.microk8s.daemon-k8s-dqlite.service"],
        capture_output=True,
        check=False,
    )
    if result.stdout.decode().strip() == "active":
        return "control-plane"
    return "worker"
