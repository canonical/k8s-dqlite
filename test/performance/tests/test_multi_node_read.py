#
# Copyright 2025 Canonical, Ltd.
#
import logging
import pytest
from typing import List
from test_util import harness, metrics, util, config, kube_burner

LOG = logging.getLogger(__name__)


def configure_argocd(control_plane: harness.Instance):
    """""Configures ArgoCD on the instance."""

    LOG.info("Create argocd namespace")
    control_plane.exec(
        ["k8s", "kubectl", "create", "namespace", "argocd"]
    )

    LOG.info("Apply ArogCD manifests")
    control_plane.exec(
        ["k8s", "kubectl", "apply", "-n", "argocd", "-f", config.ARGOCD_MANIFESTS]
    )

    LOG.info("Waiting for ArgoCD application controller pod to show up...")
    util.stubbornly(retries=3, delay_s=10).on(control_plane).until(
        lambda p: "argocd-application-controller" in p.stdout.decode()
    ).exec(["k8s", "kubectl", "get", "pod", "-n", "argocd", "-o", "json"])
    LOG.info("ArgoCD application controller pod showed up")

    LOG.info("Wait for all pods in argocd namespace to be ready")
    util.stubbornly(retries=3, delay_s=1).on(control_plane).exec(
        [
            "k8s",
            "kubectl",
            "wait",
            "--for=condition=ready",
            "pod",
            "--all",
            "-n",
            "argocd",
            "--timeout",
            "180s",
        ]
    )

@pytest.mark.node_count(3)
@pytest.mark.bootstrap_config((config.MANIFESTS_DIR / "bootstrap-all.yaml").read_text())
def test_three_node_read_load(instances: List[harness.Instance]):
    cluster_node = instances[0]
    joining_node = instances[1]
    joining_node_2 = instances[2]

    join_token = util.get_join_token(cluster_node, joining_node)
    join_token_2 = util.get_join_token(cluster_node, joining_node_2)

    assert join_token != join_token_2

    util.join_cluster(joining_node, join_token)
    util.join_cluster(joining_node_2, join_token_2)

    util.wait_until_k8s_ready(cluster_node, instances)
    nodes = util.ready_nodes(cluster_node)
    assert len(nodes) == 3, "nodes should have joined cluster"

    assert "control-plane" in util.get_local_node_status(cluster_node)
    assert "control-plane" in util.get_local_node_status(joining_node)
    assert "control-plane" in util.get_local_node_status(joining_node_2)

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
        # Collect the metrics
        metrics.stop_metrics(instances, process_dict)
        metrics.pull_metrics(instances, "three-node-read")
