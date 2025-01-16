#
# Copyright 2024 Canonical, Ltd.
#
from test_util import harness, metrics


def test_single_node_load(session_instance: harness.Instance):
    """Test the performance of a single node cluster with all features enabled."""
    metrics.configure_kube_burner(session_instance)
    process_dict = metrics.collect_metrics([session_instance])
    try:
        metrics.run_kube_burner(session_instance)
    finally:
        # Collect the metrics even if kube-burner fails.
        metrics.stop_metrics([session_instance], process_dict)
        metrics.pull_metrics([session_instance], "single-node")
