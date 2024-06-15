package embedded

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (e *embedded) NewClient() (*clientv3.Client, error) {
	return clientv3.New(e.clientConfig)
}

func (e *embedded) NewLocalClient() (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints: []string{e.config.AdvertiseClientUrls[0].String()},
		TLS:       e.clientConfig.TLS.Clone(),
	})
}
