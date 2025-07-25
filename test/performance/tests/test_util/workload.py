import logging

from test_util import config, harness, util

LOG = logging.getLogger(__name__)

ARGOCD_MANIFESTS = "https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml"

def configure_argocd(control_plane: harness.Instance):
    """""Configures ArgoCD on the instance."""

    LOG.info("Create argocd namespace")
    control_plane.exec(
        ["k8s", "kubectl", "create", "namespace", "argocd"]
    )

    LOG.info("Apply ArogCD manifests")
    control_plane.exec(
        ["k8s", "kubectl", "apply", "-n", "argocd", "-f", ARGOCD_MANIFESTS]
    )

    LOG.info("Waiting for ArgoCD application controller pod to show up...")
    util.stubbornly(retries=3, delay_s=10).on(control_plane).until(
        lambda p: "argocd-application-controller" in p.stdout.decode()
    ).exec(["k8s", "kubectl", "get", "pod", "-n", "argocd", "-o", "json"])
    LOG.info("ArgoCD application controller pod showed up")

    LOG.info("Wait for all pods in argocd namespace to be ready")
    util.stubbornly(retries=3, delay_s=1).on(control_plane).exec(
        [
            "k8s",
            "kubectl",
            "wait",
            "--for=condition=ready",
            "pod",
            "--all",
            "-n",
            "argocd",
            "--timeout",
            "180s",
        ]
    )