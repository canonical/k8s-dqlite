#
# Copyright 2024 Canonical, Ltd.#
import logging
import os
import shlex
import subprocess
from pathlib import Path
from typing import List

from test_util import config
from test_util.harness import Harness, HarnessError, Instance
from test_util.util import run, stubbornly, run_popen

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

        self._configure_profile(self.profile, config.LXD_PROFILE)

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

    def _configure_profile(self, profile_name: str, profile_config: str):
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
            run(
                ["lxc", "profile", "edit", profile_name],
                input=profile_config.encode(),
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

    def exec(self, instance_id: str, command: list, **kwargs):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        LOG.debug("Execute command %s in instance %s", command, instance_id)
        # Check if the command contains any special shell characters that need special handling
        special_characters = ['$', '>', '&', '!', '~', '||', '&&', '|']
        if any(char in ' '.join(command) for char in special_characters):
            # If the command contains special characters, directly join it as a string without shlex
            command_str = " ".join(command)  # This avoids shlex.join
        else:
            # Otherwise, use shlex.join to join the command normally
            command_str = shlex.join(command)
        return run(
            ["lxc", "shell", instance_id, "--", "bash", "-c", command_str],
            **kwargs,
        )
    
    # Updated `exec` method using `Popen` for background commands.
    def exec_with_popen(self, instance_id: str, command: list, **kwargs):
        if instance_id not in self.instances:
            raise HarnessError(f"unknown instance {instance_id}")

        LOG.debug("Execute command %s in instance %s", command, instance_id)
        # Check if the command contains any special shell characters that need special handling
        special_characters = ['$', '>', '&', '!', '~', '||', '&&', '|']
        if any(char in ' '.join(command) for char in special_characters):
            # If the command contains special characters, directly join it as a string without shlex
            command_str = " ".join(command)
        else:
            # Otherwise, use shlex.join to join the command normally
            command_str = shlex.join(command)
        return run_popen(
            ["lxc", "shell", instance_id, "--", "bash", "-c", command_str],
            **kwargs,
        )

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
