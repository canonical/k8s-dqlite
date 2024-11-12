#
# Copyright 2024 Canonical, Ltd.
#
import logging
from typing import List

import pytest
from test_util import config, harness, util

LOG = logging.getLogger(__name__)


@pytest.mark.node_count(3)
def test_three_node_load(instances: List[harness.Instance]):
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

    configure_kube_burner(cluster_node)
    process_dict = collect_metrics(instances)
    run_kube_burner(cluster_node)
    stop_metrics(instances, process_dict)
    pull_metrics(instances)


def test_single_node_load(session_instance: harness.Instance):
    """Test the performance of a single node cluster with all features enabled."""
    configure_kube_burner(session_instance)
    process_dict = collect_metrics([session_instance])
    run_kube_burner(session_instance)
    stop_metrics([session_instance], process_dict)
    pull_metrics([session_instance])


def stop_metrics(instances: List[harness.Instance], process_dict: dict):
    """Stops collecting metrics in the background from each instance."""
    for instance in instances:
        process_dict[instance.id].kill()


def collect_metrics(instances: List[harness.Instance]):
    """
    Starts collecting metrics in the background from each instance. Returns a dictionary
    with the process object for each instance.
    """
    process_dict = {}
    for instance in instances:
        pid = instance.exec(
            ["pgrep", "k8s-dqlite"], text=True, capture_output=True
        ).stdout.strip()
        instance.exec(["apt-get", "install", "-y", "sysstat"])
        subprocess = instance.exec_with_popen(
            [
                "pidstat",
                "-druh",
                "-p",
                pid,
                "1",
                ">",
                f"/root/{instance.id}_metrics.log",
            ]
        )
        process_dict[instance.id] = subprocess
    return process_dict


def pull_metrics(instances: List[harness.Instance]):
    """Pulls metrics file from each instance to the local machine."""
    for instance in instances:
        instance.pull_file(
            f"/root/{instance.id}_metrics.log", f"./{instance.id}_metrics.log"
        )


def configure_kube_burner(instance: harness.Instance):
    """Downloads and sets up `kube-burner` on each instance if it's not already present."""
    if (
        not instance.exec(["test", "-f", "/root/kube-burner"], check=False).returncode
        == 0
    ):
        url = "https://github.com/kube-burner/kube-burner/releases/download/v1.2/kube-burner-1.2-Linux-x86_64.tar.gz"
        instance.exec(["wget", url])
        instance.exec(
            ["tar", "-zxvf", "kube-burner-1.2-Linux-x86_64.tar.gz", "kube-burner"]
        )
        instance.exec(["rm", "kube-burner-1.2-Linux-x86_64.tar.gz"])
        instance.exec(["chmod", "+x", "/root/kube-burner"])
    instance.exec(["mkdir", "-p", "/root/templates"])
    instance.send_file(
        (config.MANIFESTS_DIR / "api-intensive.yaml").as_posix(),
        "/root/api-intensive.yaml",
    )
    instance.send_file(
        (config.MANIFESTS_DIR / "secret.yaml").as_posix(), "/root/secret.yaml"
    )
    instance.send_file(
        (config.MANIFESTS_DIR / "configmap.yaml").as_posix(), "/root/configmap.yaml"
    )


def run_kube_burner(instance: harness.Instance):
    """Copies kubeconfig and runs kube-burner on the instance."""
    instance.exec(["mkdir", "-p", "/root/.kube"])
    instance.exec(["k8s", "config", ">", "/root/.kube/config"])
    instance.exec(["/root/kube-burner", "init", "-c", "/root/api-intensive.yaml"])
