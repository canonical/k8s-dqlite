#
# Copyright 2025 Canonical, Ltd.
#
from typing import List

import pytest
from test_util import config, harness, metrics, util


@pytest.mark.bootstrap_config(
    (config.MANIFESTS_DIR / "bootstrap-profiling.yaml").read_text()
    if config.ENABLE_PROFILING
    else None
)
@pytest.mark.node_count(3)
def test_three_node_load(instances: List[harness.Instance]):
    cluster_node = instances[0]
    joining_node = instances[1]
    joining_node_2 = instances[2]

    join_token = util.get_join_token(cluster_node, joining_node)
    join_token_2 = util.get_join_token(cluster_node, joining_node_2)

    assert join_token != join_token_2

    if config.ENABLE_PROFILING:
        cp_join_cfg = (config.MANIFESTS_DIR / "join-cp-profiling.yaml").read_text()
    else:
        cp_join_cfg = ""

    util.join_cluster(joining_node, join_token, cp_join_cfg)
    util.join_cluster(joining_node_2, join_token_2, cp_join_cfg)

    util.wait_until_k8s_ready(cluster_node, instances)
    nodes = util.ready_nodes(cluster_node)
    assert len(nodes) == 3, "nodes should have joined cluster"

    assert "control-plane" in util.get_local_node_status(cluster_node)
    assert "control-plane" in util.get_local_node_status(joining_node)
    assert "control-plane" in util.get_local_node_status(joining_node_2)

    metrics.configure_kube_burner(cluster_node)
    process_dict = metrics.collect_metrics(instances)
    try:
        metrics.run_kube_burner(cluster_node)
    finally:
        # Collect the metrics even if kube-burner fails.
        metrics.stop_metrics(instances, process_dict)
        metrics.pull_metrics(instances, "three-node")
