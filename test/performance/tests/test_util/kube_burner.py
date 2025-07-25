#
# Copyright 2025 Canonical, Ltd.
#
import logging
import os
import time
import subprocess

from test_util import config, harness

LOG = logging.getLogger(__name__)



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

def copy_from_templates(instance: harness.Instance, names: list[str]):
    """Copies files from the templates directory to the instance."""
    for name in names:
        instance.send_file(
            (config.MANIFESTS_DIR / name).as_posix(),
            f"/root/templates/{name}",
        )


def run_kube_burner(
    instance: harness.Instance, 
    kube_burner_config: str,
    iterations: int = config.KUBE_BURNER_ITERATIONS,
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
                    f"/root/templates/{kube_burner_config}",
                ],
                text=True, capture_output=True
            )
        except Exception as ex:
            # We'll continue the loop even after encountering failures
            # in order to determine if this is a transient failure or if the
            # dqlite service was completely compromised (e.g. deadlock or crash).
            LOG.exception("kube-burner job failed, continuing...")
            if isinstance(ex, subprocess.CalledProcessError):
                LOG.warning(f"  rc={ex.returncode}")
                LOG.warning(f"  stdout={ex.stdout}")
                LOG.warning(f"  stderr={ex.stderr}")
            raised_exc = ex

    # Raise encountered exceptions, if any.
    if raised_exc:
        raise raised_exc
