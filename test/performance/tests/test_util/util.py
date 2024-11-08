#
# Copyright 2024 Canonical, Ltd.#
import json
import logging
import re
import shlex
import subprocess
import urllib.request
from functools import partial
from pathlib import Path
from typing import Any, Callable, List, Mapping, Optional, Union

import pytest
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_never,
    wait_fixed,
)
from test_util import config, harness

LOG = logging.getLogger(__name__)
RISKS = ["stable", "candidate", "beta", "edge"]
TRACK_RE = re.compile(r"^(\d+)\.(\d+)(\S*)$")


def run(command: list, **kwargs) -> subprocess.CompletedProcess:
    """Log and run command."""
    kwargs.setdefault("check", True)

    LOG.debug("Execute command %s (kwargs=%s)", shlex.join(command), kwargs)
    return subprocess.run(command, **kwargs)


def run_popen(command: list, **kwargs) -> subprocess.Popen:
    """Log and run command."""
    LOG.debug("Execute command %s (kwargs=%s)", shlex.join(command), kwargs)
    return subprocess.Popen(command, **kwargs)


def stubbornly(
    retries: Optional[int] = None,
    delay_s: Optional[Union[float, int]] = None,
    exceptions: Optional[tuple] = None,
    **retry_kds,
):
    """
    Retry a command for a while, using tenacity

    By default, retry immediately and forever until no exceptions occur.

    Some commands need to execute until they pass some condition
    > stubbornly(*retry_args).until(*some_condition).exec(*some_command)

    Some commands need to execute until they complete
    > stubbornly(*retry_args).exec(*some_command)

    : param    retries              int: convenience param to use stop=retry.stop_after_attempt(<int>)
    : param    delay_s        float|int: convenience param to use wait=retry.wait_fixed(delay_s)
    : param exceptions Tuple[Exception]: convenience param to use retry=retry.retry_if_exception_type(exceptions)
    : param retry_kds           Mapping: direct interface to all tenacity arguments for retrying
    """

    def _before_sleep(retry_state: RetryCallState):
        attempt = retry_state.attempt_number
        tries = f"/{retries}" if retries is not None else ""
        LOG.info(
            f"Attempt {attempt}{tries} failed. Error: {retry_state.outcome.exception()}"
        )
        LOG.info(f"Retrying in {delay_s} seconds...")

    _waits = wait_fixed(delay_s) if delay_s is not None else wait_fixed(0)
    _stops = stop_after_attempt(retries) if retries is not None else stop_never
    _exceptions = exceptions or (Exception,)  # default to retry on all exceptions

    _retry_args = dict(
        wait=_waits,
        stop=_stops,
        retry=retry_if_exception_type(_exceptions),
        before_sleep=_before_sleep,
    )
    # Permit any tenacity retry overrides from these ^defaults
    _retry_args.update(retry_kds)

    class Retriable:
        def __init__(self) -> None:
            self._condition = None
            self._run = partial(run, capture_output=True)

        @retry(**_retry_args)
        def exec(
            self,
            command_args: List[str],
            **command_kwds,
        ):
            """
            Execute a command against a harness or locally with subprocess to be retried.

            :param  List[str]        command_args: The command to be executed, as a str or list of str
            :param Mapping[str,str]      command_kwds: Additional keyword arguments to be passed to exec
            """

            try:
                resp = self._run(command_args, **command_kwds)
            except subprocess.CalledProcessError as e:
                LOG.warning(f"  rc={e.returncode}")
                LOG.warning(f"  stdout={e.stdout.decode()}")
                LOG.warning(f"  stderr={e.stderr.decode()}")
                raise
            if self._condition:
                assert self._condition(resp), "Failed to meet condition"
            return resp

        def on(self, instance: harness.Instance) -> "Retriable":
            """
            Target the command at some instance.

            :param instance Instance: Instance on a test harness.
            """
            self._run = partial(instance.exec, capture_output=True)
            return self

        def until(
            self, condition: Callable[[subprocess.CompletedProcess], bool] = None
        ) -> "Retriable":
            """
            Test the output of the executed command against an expected response

            :param Callable condition: a callable which returns a truth about the command output
            """
            self._condition = condition
            return self

    return Retriable()


def _as_int(value: Optional[str]) -> Optional[int]:
    """Convert a string to an integer."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def setup_k8s_snap(
    instance: harness.Instance,
    tmp_path: Path,
    snap: Optional[str] = None,
    connect_interfaces=True,
):
    """Installs and sets up the snap on the given instance and connects the interfaces.

    Args:
        instance:   instance on which to install the snap
        tmp_path:   path to store the snap on the instance
        snap: choice of track, channel, revision, or file path
            a snap track to install
            a snap channel to install
            a snap revision to install
            a path to the snap to install
    """
    cmd = ["snap", "install", "--classic"]
    which_snap = snap or config.SNAP

    if not which_snap:
        pytest.fail("Set TEST_SNAP to the channel, revision, or path to the snap")

    if isinstance(which_snap, str) and which_snap.startswith("/"):
        LOG.info("Install k8s snap by path")
        snap_path = (tmp_path / "k8s.snap").as_posix()
        instance.send_file(which_snap, snap_path)
        cmd += ["--dangerous", snap_path]
    elif snap_revision := _as_int(which_snap):
        LOG.info("Install k8s snap by revision")
        cmd += [config.SNAP_NAME, "--revision", snap_revision]
    elif "/" in which_snap or which_snap in RISKS:
        LOG.info("Install k8s snap by specific channel: %s", which_snap)
        cmd += [config.SNAP_NAME, "--channel", which_snap]
    elif channel := tracks_least_risk(which_snap, instance.arch):
        LOG.info("Install k8s snap by least risky channel: %s", channel)
        cmd += [config.SNAP_NAME, "--channel", channel]

    instance.exec(cmd)
    if connect_interfaces:
        LOG.info("Ensure k8s interfaces and network requirements")
        instance.exec(["/snap/k8s/current/k8s/hack/init.sh"], stdout=subprocess.DEVNULL)


def wait_until_k8s_ready(
    control_node: harness.Instance,
    instances: List[harness.Instance],
    retries: int = config.DEFAULT_WAIT_RETRIES,
    delay_s: int = config.DEFAULT_WAIT_DELAY_S,
    node_names: Mapping[str, str] = {},
):
    """
    Validates that the K8s node is in Ready state.

    By default, the hostname of the instances is used as the node name.
    If the instance name is different from the hostname, the instance name should be passed to the
    node_names dictionary, e.g. {"instance_id": "node_name"}.
    """
    for instance in instances:
        if instance.id in node_names:
            node_name = node_names[instance.id]
        else:
            node_name = hostname(instance)

        result = (
            stubbornly(retries=retries, delay_s=delay_s)
            .on(control_node)
            .until(lambda p: " Ready" in p.stdout.decode())
            .exec(["k8s", "kubectl", "get", "node", node_name, "--no-headers"])
        )
    LOG.info("Kubelet registered successfully!")
    LOG.info("%s", result.stdout.decode())


def wait_for_dns(instance: harness.Instance):
    LOG.info("Waiting for DNS to be ready")
    instance.exec(["k8s", "x-wait-for", "dns"])


def wait_for_network(instance: harness.Instance):
    LOG.info("Waiting for network to be ready")
    instance.exec(["k8s", "x-wait-for", "network"])


def hostname(instance: harness.Instance) -> str:
    """Return the hostname for a given instance."""
    resp = instance.exec(["hostname"], capture_output=True)
    return resp.stdout.decode().strip()


def get_local_node_status(instance: harness.Instance) -> str:
    resp = instance.exec(["k8s", "local-node-status"], capture_output=True)
    return resp.stdout.decode().strip()


def get_nodes(control_node: harness.Instance) -> List[Any]:
    """Get a list of existing nodes.

    Args:
        control_node: instance on which to execute check

    Returns:
        list of nodes
    """
    result = control_node.exec(
        ["k8s", "kubectl", "get", "nodes", "-o", "json"], capture_output=True
    )
    assert result.returncode == 0, "Failed to get nodes with kubectl"
    node_list = json.loads(result.stdout.decode())
    assert node_list["kind"] == "List", "Should have found a list of nodes"
    return [node for node in node_list["items"]]


def ready_nodes(control_node: harness.Instance) -> List[Any]:
    """Get a list of the ready nodes.

    Args:
        control_node: instance on which to execute check

    Returns:
        list of nodes
    """
    return [
        node
        for node in get_nodes(control_node)
        if all(
            condition["status"] == "False"
            for condition in node["status"]["conditions"]
            if condition["type"] != "Ready"
        )
    ]


# Create a token to join a node to an existing cluster
def get_join_token(
    initial_node: harness.Instance, joining_cplane_node: harness.Instance, *args: str
) -> str:
    out = initial_node.exec(
        ["k8s", "get-join-token", joining_cplane_node.id, *args],
        capture_output=True,
    )
    return out.stdout.decode().strip()


# Join an existing cluster.
def join_cluster(instance: harness.Instance, join_token: str):
    instance.exec(["k8s", "join-cluster", join_token])

def tracks_least_risk(track: str, arch: str) -> str:
    """Determine the snap channel with the least risk in the provided track.

    Args:
        track: the track to determine the least risk channel for
        arch: the architecture to narrow the revision

    Returns:
        the channel associated with the least risk
    """
    LOG.debug("Determining least risk channel for track: %s on %s", track, arch)
    if track == "latest":
        return f"latest/edge/{config.FLAVOR or 'classic'}"

    INFO_URL = f"https://api.snapcraft.io/v2/snaps/info/{config.SNAP_NAME}"
    HEADERS = {
        "Snap-Device-Series": "16",
        "User-Agent": "Mozilla/5.0",
    }

    req = urllib.request.Request(INFO_URL, headers=HEADERS)
    with urllib.request.urlopen(req) as response:
        snap_info = json.loads(response.read().decode())

    risks = [
        channel["channel"]["risk"]
        for channel in snap_info["channel-map"]
        if channel["channel"]["track"] == track
        and channel["channel"]["architecture"] == arch
    ]
    if not risks:
        raise ValueError(f"No risks found for track: {track}")
    risk_level = {"stable": 0, "candidate": 1, "beta": 2, "edge": 3}
    channel = f"{track}/{min(risks, key=lambda r: risk_level[r])}"
    LOG.info("Least risk channel from track %s is %s", track, channel)
    return channel
