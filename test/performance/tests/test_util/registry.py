#
# Copyright 2026 Canonical, Ltd.
#
"""Pull-through registry mirror, ported from canonical/k8s-snap."""
import base64
import logging
from string import Template
from typing import List, Optional

from test_util import config, harness, util

LOG = logging.getLogger(__name__)


class Mirror:
    def __init__(
        self,
        name: str,
        port: int,
        remote: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.name = name
        self.port = port
        self.remote = remote
        self.username = username or ""
        self.password = password or ""


class Registry:
    """A pull-through registry mirror running in a dedicated LXD container.

    Spins up one CNCF Distribution registry process per upstream registry
    (docker.io, ghcr.io, quay.io, …).  Test containers are then configured to
    pull through the mirror so Docker Hub rate limits are never hit.
    """

    def __init__(self, h: harness.Harness):
        self._harness = h
        self._mirrors: List[Mirror] = _get_configured_mirrors()
        self.instance: Optional[harness.Instance] = h.new_instance()
        self._ip = util.get_default_ip(self.instance)

        arch = self.instance.arch
        registry_tar = f"registry_{config.REGISTRY_VERSION[1:]}_linux_{arch}.tar.gz"
        self.instance.exec([
            "bash", "-c",
            f"curl -fsSL {config.REGISTRY_URL}/{config.REGISTRY_VERSION}/{registry_tar}"
            f" -o /tmp/{registry_tar}",
        ])
        self.instance.exec([
            "bash", "-c",
            f"tar xzvf /tmp/{registry_tar} -C /bin/ registry",
        ])

        self._add_mirrors()

    def _write_file(self, content: str, remote_path: str):
        """Write *content* to *remote_path* on the registry instance using base64."""
        encoded = base64.b64encode(content.encode()).decode()
        self.instance.exec([
            "bash", "-c",
            f"echo {encoded} | base64 -d > {remote_path}",
        ])

    def _add_mirrors(self):
        for mirror in self._mirrors:
            substitutes = {
                "NAME": mirror.name,
                "PORT": str(mirror.port),
                "REMOTE": mirror.remote,
                "USERNAME": mirror.username,
                "PASSWORD": mirror.password,
            }

            self.instance.exec(["bash", "-c", "mkdir -p /etc/distribution"])
            self.instance.exec([
                "bash", "-c",
                f"mkdir -p /var/lib/registry/{mirror.name}",
            ])

            cfg = Template(
                (config.REGISTRY_DIR / "registry-config.yaml").read_text()
            ).substitute(substitutes)
            self._write_file(cfg, f"/etc/distribution/{mirror.name}.yaml")

            svc = Template(
                (config.REGISTRY_DIR / "registry.service").read_text()
            ).substitute(substitutes)
            self._write_file(
                svc,
                f"/etc/systemd/system/registry-{mirror.name}.service",
            )

            self.instance.exec(["bash", "-c", "systemctl daemon-reload"])
            self.instance.exec([
                "bash", "-c",
                f"systemctl enable --now registry-{mirror.name}.service",
            ])

    @property
    def ip(self) -> str:
        return self._ip

    @property
    def mirrors(self) -> List[Mirror]:
        return self._mirrors

    def apply_to_microk8s(self, instance: harness.Instance):
        """Configure MicroK8s containerd on *instance* to use this registry mirror.

        MicroK8s containerd already has:
          config_path = "/var/snap/microk8s/current/args/certs.d"
        so we just write a hosts.toml per registry under that path.
        """
        hosts_tmpl = Template(
            (config.REGISTRY_DIR / "hosts.toml").read_text()
        )
        for mirror in self._mirrors:
            hosts_toml = hosts_tmpl.substitute(IP=self._ip, PORT=mirror.port)
            certs_dir = (
                f"/var/snap/microk8s/current/args/certs.d/{mirror.name}"
            )
            instance.exec(["bash", "-c", f"mkdir -p {certs_dir}"])
            encoded = base64.b64encode(hosts_toml.encode()).decode()
            instance.exec([
                "bash", "-c",
                f"echo {encoded} | base64 -d > {certs_dir}/hosts.toml",
            ])
        LOG.info(
            "Configured MicroK8s containerd on %s to use registry mirror at %s",
            instance.id,
            self._ip,
        )

    def cleanup(self):
        if self.instance:
            LOG.info("Cleaning up registry instance: %s", self.instance.id)
            self.instance.delete_instance()
            self.instance = None


def _get_configured_mirrors() -> List[Mirror]:
    mirrors = []
    for d in config.MIRROR_LIST:
        for field in ("name", "port", "remote"):
            if field not in d:
                raise ValueError(
                    f"Invalid TEST_MIRROR_LIST entry — missing field: {field}"
                )
        mirrors.append(
            Mirror(
                d["name"],
                int(d["port"]),
                d["remote"],
                d.get("username"),
                d.get("password"),
            )
        )
    return mirrors
