# K8s-dqlite Versioning

K8s-dqlite uses [semantic versioning](https://semver.org/). This means that the version number is composed
of three numbers: `MAJOR.MINOR.PATCH`. The version number is incremented based on the following rules:

- `MAJOR` is incremented when incompatible database schema changes are made.
- `MINOR` is incremented when a new features is added in a backwards-compatible manner.
- `PATCH` is incremented when backwards-compatible bug fixes are made.

## K8s-dqlite versions and Kubernetes versions

K8s-dqlite versions are associated with one or more Kubernetes versions in use by the [MicroK8s](https://github.com/canonical/microk8s) and [Canonical Kubernetes](https://github.com/canonical/k8s-snap) project.
Here is an overview that shows which k8s-dqlite version aligns with which supported Kubernetes version:

| K8s-dqlite Tag     | K8s-dqlite Branch  | Kubernetes Version |
|--------------------|--------------------|--------------------|
| 1.1.12             | v1.1               | 1.28-1.30          |
| 1.2.1              | v1.2               | 1.31               |
| 1.3.1              | v1.3               | 1.32-1.33          |
| 1.4.1              | NA                 | NA                 |
| 1.5.0              | NA                 | NA                 |
| 1.6.0              | master             | NA                 |

Note: K8s-dqlite tags `v1.1.7` and branch `1.28` are prior to the major refactor from [Canonical kine](https://github.com/canonical/kine).
All supported products prior to k8s `1.31` use the `v1.1.x` tag for which the `v1.1` branch tracks its patches.
The patch release `1.1.12`, `1.2.1` and `v1.3.1` contains a memory leak fix.
