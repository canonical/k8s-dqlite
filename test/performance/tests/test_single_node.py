#
# Copyright 2024 Canonical, Ltd.
#
import logging

from test_util import harness, metrics

LOG = logging.getLogger(__name__)


def test_single_node_load(session_instance: harness.Instance):
    """Test the performance of a single node cluster with all features enabled."""
    metrics.configure_kube_burner(session_instance)
    process_dict = metrics.collect_metrics([session_instance])
    metrics.run_kube_burner(session_instance)
    metrics.stop_metrics([session_instance], process_dict)
    metrics.pull_metrics([session_instance], "single-node")
