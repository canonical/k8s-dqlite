package limited

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

var _ etcdserverpb.MaintenanceServer = (*KVServerBridge)(nil)

// The emulated etcd version is returned on a call to the status endpoint. The version 3.5.13, indicates support for the watch progress notifications.
// See: https://github.com/kubernetes/kubernetes/blob/beb696c2c9467dbc44cbaf35c5a4a3daf0321db3/staging/src/k8s.io/apiserver/pkg/storage/feature/feature_support_checker.go#L157
const emulatedEtcdVersion = "3.5.13"

func (s *KVServerBridge) Alarm(context.Context, *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	return nil, fmt.Errorf("alarm is not supported")
}

func (s *KVServerBridge) Status(ctx context.Context, r *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	size, err := s.limited.dbSize(ctx)
	if err != nil {
		return nil, err
	}
	return &etcdserverpb.StatusResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		DbSize:  size,
		Version: emulatedEtcdVersion,
	}, nil
}

func (s *KVServerBridge) Defragment(context.Context, *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	return nil, fmt.Errorf("defragment is not supported")
}

func (s *KVServerBridge) Hash(context.Context, *etcdserverpb.HashRequest) (*etcdserverpb.HashResponse, error) {
	return nil, fmt.Errorf("hash is not supported")
}

func (s *KVServerBridge) HashKV(context.Context, *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	return nil, fmt.Errorf("hash kv is not supported")
}

func (s *KVServerBridge) Snapshot(*etcdserverpb.SnapshotRequest, etcdserverpb.Maintenance_SnapshotServer) error {
	return fmt.Errorf("snapshot is not supported")
}

func (s *KVServerBridge) MoveLeader(context.Context, *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	return nil, fmt.Errorf("move leader is not supported")
}

func (s *KVServerBridge) Downgrade(context.Context, *etcdserverpb.DowngradeRequest) (*etcdserverpb.DowngradeResponse, error) {
	return nil, fmt.Errorf("downgrade is not supported")
}
