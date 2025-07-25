#
# Copyright 2025 Canonical, Ltd.
#
from typing import List

import pytest
from test_util import harness, metrics, util, config, workload, kube_burner


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
    workload.configure_argocd(cluster_node)
    process_dict = metrics.collect_metrics(instances)
    try:
        kube_burner.run_kube_burner(cluster_node, "read-intensive.yaml")
    finally:
        # Collect the metrics
        metrics.stop_metrics(instances, process_dict)
        metrics.pull_metrics(instances, "three-node-read")
