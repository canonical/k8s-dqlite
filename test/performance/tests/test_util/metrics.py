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

        if config.OTEL_ENABLED:
            for otel_file in ["k8s-dqlite-metrics", "k8s-dqlite-traces"]:
                out_file = os.path.join(out_dir, f"{file_prefix}otel-{otel_file}.txt")
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


