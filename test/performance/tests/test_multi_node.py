#
# Copyright 2026 Canonical, Ltd.
#
import os
from typing import List

import pytest
from test_util import config, harness, kube_burner, metrics, util

USE_MICROK8S = os.environ.get("USE_MICROK8S", "").lower() == "true"

if USE_MICROK8S:
    import microk8s_util


@pytest.mark.node_count(3)
@pytest.mark.bootstrap_config((config.MANIFESTS_DIR / "bootstrap-all.yaml").read_text())
def test_three_node_load(instances: List[harness.Instance]):
    cluster_node = instances[0]
    joining_node = instances[1]
    joining_node_2 = instances[2]

    if USE_MICROK8S:
        join_token = microk8s_util.get_join_token(cluster_node, joining_node)
        join_token_2 = microk8s_util.get_join_token(cluster_node, joining_node_2)
    else:
        join_token = util.get_join_token(cluster_node, joining_node)
        join_token_2 = util.get_join_token(cluster_node, joining_node_2)

    assert join_token != join_token_2

    if USE_MICROK8S:
        microk8s_util.join_cluster(joining_node, join_token)
        microk8s_util.join_cluster(joining_node_2, join_token_2)
        microk8s_util.wait_until_ready(cluster_node, instances)
        nodes = microk8s_util.ready_nodes(cluster_node)
        assert len(nodes) == 3, "nodes should have joined cluster"
        assert "control-plane" in microk8s_util.get_local_node_status(cluster_node)
        assert "control-plane" in microk8s_util.get_local_node_status(joining_node)
        assert "control-plane" in microk8s_util.get_local_node_status(joining_node_2)
    else:
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
            "api-intensive.yaml",
            "configmap.yaml",
            "secret.yaml",
        ],
    )
    process_dict = metrics.collect_metrics(instances)
    try:
        kube_burner.run_kube_burner(cluster_node, "api-intensive.yaml")
    finally:
        # Collect the metrics even if kube-burner fails.
        metrics.stop_metrics(instances, process_dict)
        metrics.pull_metrics(instances, "three-node")
