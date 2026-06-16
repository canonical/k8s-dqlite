#
# Copyright 2026 Canonical, Ltd.
#
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import List

import pylxd

from test_util import config
from test_util.harness import Harness, HarnessError, Instance
from test_util.util import run, run_popen, stubbornly

LOG = logging.getLogger(__name__)


class LXDHarness(Harness):
    """A Harness that creates an LXD container for each instance."""

    name = "lxd"

    def next_id(self) -> int:
        self._next_id += 1
        return self._next_id

    def __init__(self):
        super(LXDHarness, self).__init__()

        self._next_id = 0
        self.profile = config.LXD_PROFILE_NAME
        self.image = config.LXD_IMAGE
        self.instances = set()
        self._lxd = pylxd.Client()

        self._configure_profile(self.profile)

        self._configure_network(
            "lxdbr0",
            "ipv4.address=auto",
            "ipv4.nat=true",
        )

        LOG.debug(
            "Configured LXD substrate (profile %s, image %s)", self.profile, self.image
        )

    def new_instance(self, network_type: str = "IPv4") -> Instance:
        instance_id = f"k8s-performance-{os.urandom(3).hex()}-{self.next_id()}"

        LOG.debug("Creating instance %s with image %s", instance_id, self.image)
        launch_lxd_command = [
            "lxc",
            "launch",
            self.image,
            instance_id,
            "-p",
            "default",
            "-p",
            self.profile,
        ]

        if network_type.lower() != "ipv4":
            raise HarnessError(
                f"unknown network type {network_type}, need to be one of 'IPv4'"
            )

        try:
            stubbornly(retries=3, delay_s=1).exec(launch_lxd_command)
            self.instances.add(instance_id)

        except subprocess.CalledProcessError as e:
            raise HarnessError(f"Failed to create LXD container {instance_id}") from e

        self.exec(instance_id, ["snap", "wait", "system", "seed.loaded"])
        return Instance(self, instance_id)

    def _configure_profile(self, profile_name: str):
        LOG.debug("Checking for LXD profile %s", profile_name)
        try:
            run(["lxc", "profile", "show", profile_name])
        except subprocess.CalledProcessError:
            try:
                LOG.debug("Creating LXD profile %s", profile_name)
                run(["lxc", "profile", "create", profile_name])

            except subprocess.CalledProcessError as e:
                raise HarnessError(
                    f"Failed to create LXD profile {profile_name}"
                ) from e

        try:
            LOG.debug("Configuring LXD profile %s", profile_name)
            profile = run(
                [
                    "curl",
                    "-s",
                    "https://raw.githubusercontent.com/canonical/k8s-snap"
                    "/refs/heads/main/tests/integration/lxd-profile.yaml",
                ],
                capture_output=True,
            ).stdout
            run(
                ["lxc", "profile", "edit", profile_name],
                input=profile,
            )
        except subprocess.CalledProcessError as e:
            raise HarnessError(f"Failed to configure LXD profile {profile_name}") from e

    def _configure_network(self, network_name: str, *network_args: List[str]):
        LOG.debug("Checking for LXD network %s", network_name)
        try:
            run(["lxc", "network", "show", network_name])
        except subprocess.CalledProcessError:
            try:
                LOG.debug("Creating LXD network %s", network_name)
                run(["lxc", "network", "create", network_name, *network_args])

            except subprocess.CalledProcessError as e:
                raise HarnessError(
                    f"Failed to create LXD network {network_name}"
                ) from e

    def send_file(self, instance_id: str, source: str, destination: str):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        if not Path(destination).is_absolute():
            raise HarnessError(f"path {destination} must be absolute")

        LOG.debug(
            "Copying file %s to instance %s at %s", source, instance_id, destination
        )
        try:
            self.exec(
                instance_id,
                ["mkdir", "-m=0777", "-p", Path(destination).parent.as_posix()],
                capture_output=True,
            )
            run(
                ["lxc", "file", "push", source, f"{instance_id}{destination}"],
                capture_output=True,
            )
        except subprocess.CalledProcessError as e:
            LOG.error("command {e.cmd} failed")
            LOG.error(f"  {e.returncode=}")
            LOG.error(f"  {e.stdout.decode()=}")
            LOG.error(f"  {e.stderr.decode()=}")
            raise HarnessError("failed to push file") from e

    def pull_file(self, instance_id: str, source: str, destination: str):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        if not Path(source).is_absolute():
            raise HarnessError(f"path {source} must be absolute")

        LOG.debug(
            "Copying file %s from instance %s to %s", source, instance_id, destination
        )
        try:
            run(
                ["lxc", "file", "pull", f"{instance_id}{source}", destination],
                stdout=subprocess.DEVNULL,
            )
        except subprocess.CalledProcessError as e:
            raise HarnessError("lxc file push command failed") from e

    def exec(self, instance_id: str, command: list, background: bool = False, **kwargs):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        LOG.debug("Execute command %s in instance %s", command, instance_id)

        # Wrap command in bash unless it's already a bash invocation.
        # This ensures shell operators (>, >>, |) work correctly, while
        # avoiding double-wrapping ["bash", "-c", "..."] calls.
        if command[0] == "bash":
            full_cmd = command
        else:
            full_cmd = ["bash", "-c", " ".join(command)]

        command_str = shlex.join(command)

        if background:
            # Background (fire-and-forget) tasks use the lxc CLI. The 243 issue
            # only triggers for long-running snap/microk8s commands that stream
            # substantial output; short-lived background monitors (pidstat) are
            # not affected.
            lxc_cmd = ["lxc", "exec", instance_id, "--"] + full_cmd
            return run_popen(lxc_cmd, stdin=subprocess.DEVNULL, **kwargs)

        # Extract subprocess-style kwargs that pylxd doesn't accept.
        check = kwargs.pop("check", True)
        text = kwargs.pop("text", False)
        kwargs.pop("capture_output", None)
        kwargs.pop("stdin", None)
        kwargs.pop("stdout", None)
        kwargs.pop("stderr", None)
        # Ignore any remaining kwargs (e.g. env, cwd) — not supported via pylxd.
        if kwargs:
            LOG.debug("Ignoring unsupported exec kwargs: %s", list(kwargs))

        # Use pylxd (LXD REST API) instead of the lxc CLI to avoid exit 243
        # ("websocket connection reset") that occurs when lxc exec is called
        # from within a pytest/tox subprocess where stdout is a pipe rather
        # than a TTY.
        lxd_inst = self._lxd.instances.get(instance_id)
        result = lxd_inst.execute(full_cmd)

        if result.stdout:
            for line in result.stdout.rstrip("\n").splitlines():
                LOG.debug("[%s] %s", instance_id, line)
        if result.stderr:
            for line in result.stderr.rstrip("\n").splitlines():
                LOG.debug("[%s] stderr: %s", instance_id, line)

        if text:
            stdout_val = result.stdout or ""
            stderr_val = result.stderr or ""
        else:
            stdout_val = (result.stdout or "").encode()
            stderr_val = (result.stderr or "").encode()

        completed = subprocess.CompletedProcess(
            args=command,
            returncode=result.exit_code,
            stdout=stdout_val,
            stderr=stderr_val,
        )

        if check and result.exit_code != 0:
            err = subprocess.CalledProcessError(
                result.exit_code, command_str, stdout_val, stderr_val
            )
            raise err

        return completed

    def delete_instance(self, instance_id: str):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        try:
            run(["lxc", "rm", instance_id, "--force"])
        except subprocess.CalledProcessError as e:
            raise HarnessError(f"failed to delete instance {instance_id}") from e

        self.instances.discard(instance_id)

    def cleanup(self):
        for instance_id in self.instances.copy():
            self.delete_instance(instance_id)
