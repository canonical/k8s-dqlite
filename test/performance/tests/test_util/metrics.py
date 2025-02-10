#
# Copyright 2025 Canonical, Ltd.
#
import logging
import os
from typing import List

from test_util import config, harness, util

LOG = logging.getLogger(__name__)


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
        util.stubbornly(retries=5, delay_s=3).on(instance).exec(
            ["apt-get", "install", "-y", "sysstat"]
        )
        subprocess = instance.exec(
            [
                "pidstat",
                "-druh",
                "-p",
                pid,
                "1",
                ">",
                f"/root/{instance.id}_metrics.log",
            ],
            background=True,
        )
        process_dict[instance.id] = subprocess
    return process_dict


def pull_metrics(instances: List[harness.Instance], test_name: str):
    """Pulls metrics file from each instance to the local machine."""
    for i, instance in enumerate(instances, start=1):
        out_file = f"{config.METRICS_DIR}/{config.RUN_NAME}-{i}-of{len(instances)}-{test_name}.log"
        instance.pull_file(
            f"/root/{instance.id}_metrics.log",
            out_file,
        )

        # Stop k8s-dqlite, triggering a pprof data dump. Don't start it back
        # until we've processed the data, otherwise it's going to override the file.
        instance.exec(["snap", "stop", "k8s.k8s-dqlite"])
        try:
            # Pull pprof data. We could also run "go tool pprof" on the host machine
            # but then we wouldn't have access to the binary symbols.
            instance.exec(["snap", "install", "go", "--classic"])
            # Parse the cpu profile, sorting by cumulative time.
            instance.exec(
                [
                    "go",
                    "tool",
                    "pprof",
                    "-top",
                    "-cum",
                    "/root/cpu_profile.raw",
                    ">",
                    f"/root/{instance.id}_cpu_profile.txt",
                ],
            )
            out_file = f"{config.METRICS_DIR}/{config.RUN_NAME}-{i}-of{len(instances)}-{test_name}-cpu-profile.txt"
            instance.pull_file(
                f"/root/{instance.id}_cpu_profile.txt",
                out_file,
            )
        except Exception as ex:
            LOG.exception("failed to retrieve pprof data")
        instance.exec(["snap", "start", "k8s.k8s-dqlite"])


def configure_kube_burner(instance: harness.Instance):
    """Downloads and sets up `kube-burner` on each instance if it's not already present."""
    if (
        not instance.exec(["test", "-f", "/root/kube-burner"], check=False).returncode
        == 0
    ):
        url = config.KUBE_BURNER_URL
        instance.exec(["wget", url])
        tarball_name = os.path.basename(url)
        instance.exec(["tar", "-zxvf", tarball_name, "kube-burner"])
        instance.exec(["rm", tarball_name])
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
    instance.exec(
        [
            "/root/kube-burner",
            "init",
            "--timeout",
            config.KUBE_BURNER_TIMEOUT,
            "-c",
            "/root/api-intensive.yaml",
        ]
    )
