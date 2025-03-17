#
# Copyright 2025 Canonical, Ltd.
#
import logging
import os
import time
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


def generate_graphs(test_metrics_dir: str):
    """Generate plots based on the resource usage metrics."""
    cmd = [
        "sudo",
        "Rscript",
        config.METRICS_PARSE_SCRIPT.as_posix(),
        "-p",
        test_metrics_dir,
        "-o",
        test_metrics_dir,
        "-f",
        "*metrics.log",
    ]
    util.run(cmd)


def pull_metrics(instances: List[harness.Instance], test_name: str):
    """Pulls metrics file from each instance to the local machine."""
    for i, instance in enumerate(instances, start=1):
        out_dir = os.path.join(config.METRICS_DIR, config.RUN_NAME, test_name)
        os.makedirs(out_dir, exist_ok=True)

        file_prefix = ""
        if len(instances) > 1:
            file_prefix = f"{i}-of-{len(instances)}-"

        out_file = os.path.join(out_dir, f"{file_prefix}metrics.log")
        instance.pull_file(
            f"/root/{instance.id}_metrics.log",
            out_file,
        )

        generate_graphs(out_dir)

        if config.ENABLE_OTEL:
            for otel_file in ["k8s-dqlite-metrics", "k8s-dqlite-traces"]:
                out_file = os.path.join(out_dir, f"{file_prefix}-{otel_file}.log")
                instance.pull_file(
                    f"/root/{otel_file}.txt",
                    out_file,
                )

        if config.ENABLE_PROFILING:
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
                out_file = os.path.join(out_dir, f"{file_prefix}cpu-profile.txt")
                instance.pull_file(
                    f"/root/{instance.id}_cpu_profile.txt",
                    out_file,
                )
            except Exception:
                LOG.exception("failed to retrieve pprof data")
            instance.exec(["snap", "start", "k8s.k8s-dqlite"])


def configure_kube_burner(instance: harness.Instance):
    """Downloads and sets up `kube-burner` on each instance if it's not already present."""
    if (
        not instance.exec(["test", "-f", "/root/kube-burner"], check=False).returncode
        == 0
    ):
        url = config.KUBE_BURNER_URL
        for retry in range(5):
            try:
                instance.exec(["wget", url])
                break
            except Exception as ex:
                if retry < 5:
                    time.sleep(3)
                    LOG.exception("Failed to download kube-burner, retrying...")
                else:
                    raise ex
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


def run_kube_burner(
    instance: harness.Instance, iterations: int = config.KUBE_BURNER_ITERATIONS
):
    """Copies kubeconfig and runs kube-burner on the instance."""
    instance.exec(["mkdir", "-p", "/root/.kube"])
    instance.exec(["k8s", "config", ">", "/root/.kube/config"])

    raised_exc = None
    for iteration in range(iterations):
        LOG.info("Starting kube-burner iteration %s of %s.", iteration, iterations)
        try:
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
        except Exception as ex:
            # We'll continue the loop even after encountering failures
            # in order to determine if this is a transient failure or if the
            # dqlite service was completely compromised (e.g. deadlock or crash).
            LOG.exception("kube-burner job failed, continuing...")
            raised_exc = ex

    # Raise encountered exceptions, if any.
    if raised_exc:
        raise raised_exc
