package embedded

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *embedded) NewClient() (*clientv3.Client, error) {
	return clientv3.New(e.clientConfig)
}
