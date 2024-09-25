# Develop k8s-dqlite against an active MicroK8s instance

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

7. While developing and making changes to `k8s-dqlite`, just restart k8s-dqlite

Note: When developing k8s-dqlite against Canonical Kubernetes use the following flags:

- `--storage-dir /var/snap/k8s/common/var/lib/k8s-dqlite`
- `--listen unix:///var/snap/k8s/common/var/lib/k8s-dqlite/k8s-dqlite.sock`
