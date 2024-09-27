# Development

K8s-dqlite is used in Canonical Kubernetes and MicroK8s as the translation layer between the
Kubernetes API server and the Dqlite database. This document provides instructions on how to
develop k8s-dqlite against an active MicroK8s instance or Canonical Kubernetes, view logs,
and connect to the Dqlite database.

## Develop k8s-dqlite against an active MicroK8s instance

1. Install development tools:

    ```bash
    sudo snap install go --classic
    sudo apt update
    sudo apt install build-essential -y
    ```

2. Clone k8s-dqlite repository:

    ```bash
    git clone https://github.com/canonical/k8s-dqlite
    ```

3. Install MicroK8s on the machine:

    ```bash
    sudo snap install microk8s --classic
    ```

4. Wait for MicroK8s to come up:

    ```bash
    sudo microk8s status --wait-ready
    ```

5. Stop k8s-dqlite included in the snap:

    ```bash
    sudo snap stop microk8s.daemon-k8s-dqlite --disable
    ```

6. Run k8s-dqlite from this repository:

    ```bash
    cd k8s-dqlite
    make static
    sudo ./bin/static/k8s-dqlite \
      --storage-dir /var/snap/microk8s/current/var/kubernetes/backend \
      --listen unix:///var/snap/microk8s/current/var/kubernetes/backend/kine.sock:12379
    ```

7. While developing and making changes to `k8s-dqlite`, just restart k8s-dqlite.

## Develop k8s-dqlite against Canonical Kubernetes

Follow the guide for MicroK8s steps 1-5 while changing the snap name to `k8s`.

Step 5:
Stop k8s-dqlite included in the snap:

    ```bash
    sudo snap stop k8s.k8s-dqlite --disable
    ```

Step 6:
This step is different as it uses a different path for the storage directory and the listen address:

    ```bash
    cd k8s-dqlite
    make static
    sudo ./bin/static/k8s-dqlite \
    --storage-dir /var/snap/k8s/common/var/lib/k8s-dqlite \
    --listen unix:///var/snap/k8s/common/var/lib/k8s-dqlite/k8s-dqlite.sock
    ```

## Viewing logs and debugging

To view k8s-dqlite logs, you can use the `journalctl` command:

```bash
journalctl -u snap.k8s.k8s-dqlite -f
```

Or use `microk8s.daemon-k8s-dqlite` for MicroK8s.

To debug, you can set the `--debug` flag to true as explained in the [configuration](configuration.md) documentation.

## Connecting to the Dqlite Database

To connect to the Dqlite database, you can run this command for Canonical Kubernetes:

```
sudo /snap/k8s/current/bin/dqlite -s file:///var/snap/k8s/common/var/lib/k8s-dqlite/cluster.yaml -c /var/snap/k8s/common/var/lib/k8s-dqlite/cluster.crt -k /var/snap/k8s/common/var/lib/k8s-dqlite/cluster.key k8s
```

Or for MicroK8s:

```
sudo /snap/microk8s/current/bin/dqlite -s file:///var/snap/microk8s/current/var/kubernetes/backend/cluster.yaml -c /var/snap/microk8s/current/var/kubernetes/backend/cluster.crt -k /var/snap/microk8s/current/var/kubernetes/backend/cluster.key k8s
```

To find the Dqlite leader run `.leader` in the dqlite shell.

## Troubleshooting Dqlite

Sometimes it is helpful to get insights into what is happening in the dqlite layer.
To do this, you can enable debug logs. Add debug logs by editing
`/var/snap/k8s/common/args/k8s-dqlite-env` or `/var/snap/microk8s/current/args/k8s-dqlite-env` and uncomment `LIBDQLITE_TRACE=1` and `LIBRAFT_TRACE=1`. Then restart the k8s-dqlite service and check the k8s-dqlite logs.
