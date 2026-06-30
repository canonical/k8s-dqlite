#
# Copyright 2026 Canonical, Ltd.
#
import logging
from typing import List

import microk8s_util
import pytest
from test_util import config, harness, kube_burner, metrics, util

LOG = logging.getLogger(__name__)


def configure_argocd(control_plane: harness.Instance):
    """Configures ArgoCD on the instance."""
    LOG.info("Create argocd namespace")
    control_plane.exec(["microk8s", "kubectl", "create", "namespace", "argocd"])

    LOG.info("Downloading ArgoCD manifests")
    # Download with retries to avoid transient network failures from inside the container.
    control_plane.exec([
        "bash", "-c",
        "for i in 1 2 3 4 5; do "
        f"curl -fsSL '{config.ARGOCD_MANIFESTS}' -o /tmp/argocd-install.yaml && break; "
        "echo \"Download attempt $i failed, retrying in $((i * 10))s...\"; "
        "sleep $((i * 10)); "
        "done; "
        "test -f /tmp/argocd-install.yaml",
    ])

    LOG.info("Apply ArgoCD manifests")
    # Use server-side apply to avoid the "metadata.annotations: Too long" error
    # that occurs with client-side apply because the applicationsets CRD exceeds
    # the 256KB last-applied-configuration annotation limit.
    control_plane.exec([
        "microk8s", "kubectl", "apply",
        "--server-side", "--force-conflicts",
        "-n", "argocd",
        "-f", "/tmp/argocd-install.yaml",
    ])

    LOG.info("Waiting for ArgoCD application controller pod to show up...")
    util.stubbornly(retries=30, delay_s=10).on(control_plane).until(
        lambda p: "argocd-application-controller" in p.stdout.decode()
    ).exec(["microk8s", "kubectl", "get", "pod", "-n", "argocd", "-o", "json"])
    LOG.info("ArgoCD application controller pod showed up")

    LOG.info("Wait for all pods in argocd namespace to be ready")
    util.stubbornly(retries=5, delay_s=10).on(control_plane).exec(
        [
            "microk8s",
            "kubectl",
            "wait",
            "--for=condition=ready",
            "pod",
            "--all",
            "-n",
            "argocd",
            "--timeout",
            "300s",
        ],
    )


@pytest.mark.node_count(3)
@pytest.mark.bootstrap_config((config.MANIFESTS_DIR / "bootstrap-all.yaml").read_text())
def test_three_node_read_load(instances: List[harness.Instance]):
    cluster_node = instances[0]
    joining_node = instances[1]
    joining_node_2 = instances[2]

    join_token = microk8s_util.get_join_token(cluster_node, joining_node)
    join_token_2 = microk8s_util.get_join_token(cluster_node, joining_node_2)

    assert join_token != join_token_2

    microk8s_util.join_cluster(joining_node, join_token)
    microk8s_util.join_cluster(joining_node_2, join_token_2)
    microk8s_util.wait_until_ready(cluster_node, instances)

    nodes = microk8s_util.ready_nodes(cluster_node)
    assert len(nodes) == 3, "nodes should have joined cluster"
    assert "control-plane" in microk8s_util.get_local_node_status(cluster_node)
    assert "control-plane" in microk8s_util.get_local_node_status(joining_node)
    assert "control-plane" in microk8s_util.get_local_node_status(joining_node_2)

    kube_burner.configure_kube_burner(cluster_node)
    kube_burner.copy_from_templates(
        cluster_node,
        [
            "read-intensive.yaml",
            "application.yaml",
        ],
    )
    configure_argocd(cluster_node)
    process_dict = metrics.collect_metrics(instances)
    try:
        kube_burner.run_kube_burner(cluster_node, "read-intensive.yaml", 1)
    finally:
        metrics.stop_metrics(instances, process_dict)
        metrics.pull_metrics(instances, "three-node-read")
