#
# Copyright 2025 Canonical, Ltd.
#
from test_util import harness, metrics, kube_burner


def test_single_node_load(session_instance: harness.Instance):
    """Test the performance of a single node cluster with all features enabled."""
    kube_burner.configure_kube_burner(session_instance)
    kube_burner.copy_from_templates(
        session_instance,
        [
            "api-intensive.yaml",
            "configmap.yaml",
            "secret.yaml",
        ],
    )
    process_dict = metrics.collect_metrics([session_instance])
    try:
        kube_burner.run_kube_burner(session_instance, "api-intensive.yaml")
    finally:
        # Collect the metrics even if kube-burner fails.
        metrics.stop_metrics([session_instance], process_dict)
        metrics.pull_metrics([session_instance], "single-node")
