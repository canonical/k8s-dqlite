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

## Parsing performance test metrics

The performance metrics parsing and creation of graphs is done in R.

The script allows you to pass three options in order to generate the graphs:

- `-p` or `--path` - the path to the directory where the performance test results are stored. By default, this is the `./results` directory
- `-o` or `--output` - the path to the directory where the graphs will be stored. By default, this is the `./results` directory
- `-f` or `--filepattern` - the pattern of the files that will be parsed, by default these are old files ending in `.log`

The script will generate the following graphs for all files matching the pattern in the specified directory:

- `cpu` - the CPU usage of the system
- `memory` - the memory usage of the system
- `io_reads` - the number of read operations on the disk
- `io_writes` - the number of write operations on the disk

One time setup for installing R and the required packages:

```bash
sudo apt install r-base
sudo Rscript -e 'install.packages(c("ggplot2", "dplyr", "optparse"), repos="https://cloud.r-project.org")'
```

The script can be run with the following command:

```bash
cd test/performance
Rscript parse_performance_metrics.R
```
