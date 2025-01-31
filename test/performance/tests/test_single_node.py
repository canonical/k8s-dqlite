#
# Copyright 2025 Canonical, Ltd.
#
import logging

from test_util import harness, metrics

LOG = logging.getLogger(__name__)


def test_single_node_load(session_instance: harness.Instance):
    """Test the performance of a single node cluster with all features enabled."""
    metrics.configure_kube_burner(session_instance)
    process_dict = metrics.collect_metrics([session_instance])

    raised_exc = None
    for iteration in range(10):
        try:
            metrics.run_kube_burner(session_instance)
        except Exception as ex:
            LOG.exception("kube-burner failed, starting new iteration.")
            # We'll start another iteration even if the test failed to see
            # if it was a transient failure or if Dqlite was compromised.
            raised_exc = ex

    # Collect the metrics even if kube-burner fails.
    metrics.stop_metrics([session_instance], process_dict)
    metrics.pull_metrics([session_instance], "single-node")

    if raised_exc:
        raise raised_exc
