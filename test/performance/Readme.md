# Performance Testing

## Overview

End to end tests are written in Python. They are built on top of a [Harness](./tests/conftest.py) fixture so that they can run on multiple environments like LXD or in the future on the local machine.

End to end tests can be configured using environment variables. You can see all available options in [./tests/config.py](./tests/config.py).

## Running end to end tests

Running the end to end tests requires `python3` and `tox`. Install with:

```bash
sudo apt install python3-virtualenv
virtualenv .venv
. .venv/bin/activate
pip install 'tox<5'
```

Further, make sure that you have downloaded the `k8s.snap`:

```bash
sudo snap download k8s --channel=latest/edge --basename k8s
```

In general, all end to end tests will require specifying the local path to the snap package under test, using the `TEST_SNAP` environment variable. Make sure to specify the full path to the file.

End to end tests are typically run with: `cd test/performance && tox -e performance`

### Running end to end tests on LXD containers

First, make sure that you have initialized LXD:

```bash
sudo lxd init --auto
```

Then, run the tests with:

```bash
export TEST_SNAP=$PWD/k8s.snap
export TEST_SUBSTRATE=lxd

export TEST_LXD_IMAGE=ubuntu:22.04               # (optionally) specify which image to use for LXD containers
export TEST_LXD_PROFILE_NAME=k8s-performance     # (optionally) specify profile name to configure
export TEST_SKIP_CLEANUP=1                       # (optionally) do not destroy machines after tests finish

cd test/performance && tox -e performance
```
