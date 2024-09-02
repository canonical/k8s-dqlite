# K8s-dqlite Versioning

K8s-dqlite uses [semantic versioning](https://semver.org/). This means that the version number is composed
of three numbers: `MAJOR.MINOR.PATCH`. The version number is incremented based on the following rules:

- `MAJOR` is incremented when incompatible database structure changes are made.
- `MINOR` is incremented when a new k8s-version is released and new features are added in a backwards-compatible manner.
- `PATCH` is incremented when backwards-compatible bug fixes are made.

## K8s-dqlite versions and Kubernetes versions

K8s-dqlite versions are used by one or more product's Kubernetes versions.
Here is an overview that shows which k8s-dqlite version aligns with which supported Kubernetes version:

| K8s-dqlite Version | Kubernetes Version |
|--------------------|--------------------|
| 1.28 (branch)      | 1.28               |
| 1.1.11             | 1.29-1.30          |
| 1.2.0              | 1.31               |

Note: K8s-dqlite `v1.1.7` and branch `1.28` are prior to the major refactor from https://github.com/canonical/kine.