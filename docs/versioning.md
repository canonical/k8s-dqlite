# K8s-dqlite Versioning

K8s-dqlite uses [semantic versioning](https://semver.org/). This means that the version number is composed
of three numbers: `MAJOR.MINOR.PATCH`. The version number is incremented based on the following rules:

- `MAJOR` is incremented when incompatible database schema changes are made.
- `MINOR` is incremented when a new features is added in a backwards-compatible manner.
- `PATCH` is incremented when backwards-compatible bug fixes are made.

## K8s-dqlite versions and Kubernetes versions

K8s-dqlite versions are associated with one or more Kubernetes versions in use by the [MicroK8s](https://github.com/canonical/microk8s) and [Canonical Kubernetes](https://github.com/canonical/k8s-snap) project.
Here is an overview that shows which k8s-dqlite version aligns with which supported Kubernetes version:

| K8s-dqlite Tag     | K8s-dqlite branch  | Kubernetes Version |
|--------------------|--------------------|--------------------|
| NA                 | 1.28               | 1.28               |
| 1.1.11             | v1.1               | 1.29-1.30          |
| 1.2.0              | master             | 1.31               |

Note: K8s-dqlite tag `v1.1.7` and branch `1.28` are prior to the major refactor from https://github.com/canonical/kine. Patches are tracked in the `1.28` branch.
