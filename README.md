# K8s-dqlite: Dqlite Backend for Kubernetes

This project is a drop in replacement for etcd on Kubernetes.

If you're looking for a lightweight, distributed, and in-memory database to replace etcd
in your K8s cluster, Dqlite might be the solution for you.

## Key Features

- Distributed SQlite: Uses Dqlite as the backend
- In-memory: Efficient and fast
- High Availability: Built-in high availabilty with Raft
- Minimal operations: Designed to be almost zero ops

## Building

`k8s-dqlite` links with [`Dqlite`](https://github.com/canonical/dqlite). To build the project, you can create static and dynamic binaries:

### Static Build (Recommended)

```
make static
./bin/static/k8s-dqlite --help
```

### Dynamic Build

If you prefer dynamic binaries (which require Dqlite shared libraries and dependencies to be present during runtime), use the following commands:

```
make dynamic
./bin/dynamic/k8s-dqlite --help
```

## Installing

1. Prepare a directory containing the necessary configuration files:

- `init.yaml` with `Address: <host_IP>:<port_used_by_dqlite>`
- `cluster.crt` and `cluster.key` pair.

2. Use the following script to generate the required files:

```
mkdir -p /var/data/
IP="127.0.0.1"
PORT="29001"
DNS=$(/bin/hostname)

# Create init.yaml with the Dqlite listening address
echo "Address: $IP:$PORT" > /var/data/init.yaml
mkdir -p /var/tmp/

# Find the csr-dqlite.conf.template in the project
cp /config/csr-dqlite.conf.template /var/tmp/csr-dqlite.conf
sed -i 's/HOSTNAME/'"${DNS}"'/g' /var/tmp/csr-dqlite.conf
sed -i 's/HOSTIP/'"${IP}"'/g' /var/tmp/csr-dqlite.conf
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes -keyout /var/data/cluster.key -out ${SNAP_DATA}/var/data/cluster.crt -subj "/CN=k8s" -config /var/tmp/csr-dqlite.conf -extensions v3_ext
chmod -R o-rwX /var/data
```

3. Start `k8s-dqlite` by running the following command:

```
k8s-dqlite --storage-dir=/var/data/
```

By default, `k8s-dqlite` listens for connections from the Kubernetes API server at `tcp://127.0.0.1:12379`.
To change the address, use the `--listen` option.

## Configuring the API server

To point the API server to `k8s-dqlite` use the following arguments:

```
# Endpoint k8s-dqlite listens at
--etcd-servers="http://<IP>:<PORT>"
# By default --etcd-servers="http://127.0.0.1:12379"

# Point to the cluster key pair
--etcd-certfile=/data/cluster.crt
--etcd-keyfile=/var/data/cluster.key

# If you are using the snap comment in these lines
#--etcd-certfile=/var/snap/k8s-dqlite/current/var/data/cluster.crt
#--etcd-keyfile=/var/snap/k8s-dqlite/current/var/data/cluster.key
```

## Highly Available Dqlite

K8s-dqlite supports high availability by using the Raft protocol to achieve consensus through an elected leader.
Below are the steps to set up a highly available Dqlite cluster with three nodes.

### Steps

1. On the main node, verify that `k8s-dqlite` is listening on the correct IP address and port. You can check this by running:

  ```
  sudo ss -tuln | grep <the ip of the main node>:29001
  ```

  If `k8s-dqlite` is listening correctly, you should see an output indicating that
  it's bound to the node's IP and port.

2. On each joining node, prepare the environment:
   - Backup the Dqlite data directory (`/var/data`).
   - Remove the existing the Dqlite data directory.
3. Copy the `cluster.crt` and `cluster.key` from the main node and place it into the joining node's `/var/data`.
4. Create the `init.yaml` file with the following content

  ```yaml
  Cluster:
  - <the ip of the main node>:29001
  Address: <ip of the joining node>:29001
  ```

5. Start the `k8s-dqlite` process on the joining node:

  ```shell
  k8s-dqlite --storage-dir=/var/data/
  ```

6. On the main node, verify that the joining node is connected to the Dqlite cluster.

  ```shell
  dqlite -s file:///var/data/cluster.yaml -c /var/data/cluster.crt -k /var/data/cluster.key -f json k8s .cluster
  ```

7. Repeat steps 2–6 for any additional nodes you wish to join to the cluster.
