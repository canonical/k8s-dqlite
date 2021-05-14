# Dqlite for Kubernetes

This project is a drop in replacement for etcd on Kubernetes.
 
If you are looking for an in-memory database to replace etcd in your
K8s cluster dqlite might be just right for you. Here is what you are getting:

 - distributed sqlite
 - in-memory
 - high availability
 - almost zero ops

## Building

To build the project you need to build:
 - raft: follow the instructions in https://github.com/canonical/raft
 - sqlite: libsqlite-dev should be enough or you could build sqlite from https://github.com/sqlite/sqlite
 - dqlite: follow the instructions at https://github.com/canonical/dqlite

Finally, you can build this project with: 
```
go build -o k8s-dqlite -tags libsqlite3,dqlite k8s-dqlite.go
```

As getting the above dependencies right may be a challenge we have packaged everything in a snap.
You can look into `snap/snapcraft.yaml` on how each dependency is build or just
 [build the snap](https://snapcraft.io/docs/go-applications) with:
```
snapcraft
```

## Installing

Prepare a directory containing:

  - `init.yaml` with `Address: <host_IP>:<port_used_by_dqlite>`
  - `cluster.crt` and `cluster.key` pair.
  
Here is a script to build you can use:
```
mkdir -p /var/data/
IP="127.0.0.1"
PORT="29001"
DNS=$(/bin/hostname)

echo "Address: $IP:$PORT" > /var/data/init.yaml
mkdir -p /var/tmp/

# Find the csr-dqlite.conf.template in the project
cp /config/csr-dqlite.conf.template /var/tmp/csr-dqlite.conf
sed -i 's/HOSTNAME/'"${DNS}"'/g' /var/tmp/csr-dqlite.conf
sed -i 's/HOSTIP/'"${IP}"'/g' /var/tmp/csr-dqlite.conf
openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes -keyout /var/data/cluster.key -out ${SNAP_DATA}/var/data/cluster.crt -subj "/CN=k8s" -config /var/tmp/csr-dqlite.conf -extensions v3_ext
chmod -R o-rwX /var/data
```

You are now ready to start `k8s-dqlite` with:
```
k8s-dqlite --storage-dir=/var/data/
```

The `--listen` option allows you to set the endpoint where dqlite should listen to for connections from kubernetes API server.  
By default `k8s-dqlite` will be listening for connections at `tcp://127.0.0.1:12379`. 

The snap package takes care of this installation step so you may want to use the snap you build above:
```
sudo snap install ./k8s-dqlite_latest_amd64.snap --classic --dangerous
```
 
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
