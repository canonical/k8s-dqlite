package limited

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	_ etcdserverpb.KVServer    = (*KVServerBridge)(nil)
	_ etcdserverpb.WatchServer = (*KVServerBridge)(nil)
)

type KVServerBridge struct {
	notifyInterval time.Duration
	limited        *LimitedServer
}

var _ etcdserverpb.LeaseServer = &KVServerBridge{}
var _ etcdserverpb.WatchServer = &KVServerBridge{}
var _ etcdserverpb.KVServer = &KVServerBridge{}
var _ etcdserverpb.MaintenanceServer = &KVServerBridge{}

func New(limited *LimitedServer, notifyInterval time.Duration) *KVServerBridge {
	return &KVServerBridge{
		notifyInterval: notifyInterval,
		limited:        limited,
	}
}

func (k *KVServerBridge) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.KeysOnly {
		return nil, unsupported("keysOnly")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.SortOrder != 0 {
		return nil, unsupported("sortOrder")
	}

	if r.SortTarget != 0 {
		return nil, unsupported("sortTarget")
	}

	if r.Serializable {
		return nil, unsupported("serializable")
	}

	if r.KeysOnly {
		return nil, unsupported("keysOnly")
	}

	if r.MinModRevision != 0 {
		return nil, unsupported("minModRevision")
	}

	if r.MinCreateRevision != 0 {
		return nil, unsupported("minCreateRevision")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.MaxModRevision != 0 {
		return nil, unsupported("maxModRevision")
	}

	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Range", otelName))
	defer span.End()

	var (
		resp *RangeResponse
		err  error
	)

	if len(r.RangeEnd) == 0 {
		resp, err = k.get(ctx, r)
	} else {
		resp, err = k.list(ctx, r)

	}

	if err != nil {
		logrus.Errorf("error while range on %s %s: %v", r.Key, r.RangeEnd, err)
		return nil, err
	}

	rangeResponse := &etcdserverpb.RangeResponse{
		More:   resp.More,
		Count:  resp.Count,
		Header: resp.Header,
		Kvs:    toKVs(resp.Kvs...),
	}

	return rangeResponse, nil
}

func (k *KVServerBridge) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	return nil, fmt.Errorf("put is not supported")
}

func (k *KVServerBridge) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	return nil, fmt.Errorf("delete is not supported")
}

func (k *KVServerBridge) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return k.create(ctx, put)
	}
	if rev, key, ok := isDelete(txn); ok {
		return k.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return k.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return k.compact(ctx)
	}
	return nil, fmt.Errorf("unsupported transaction: %v", txn)
}

func unsupported(field string) error {
	return fmt.Errorf("%s is unsupported", field)
}

func toKVs(kvs ...*KeyValue) []*mvccpb.KeyValue {
	if len(kvs) == 0 || kvs[0] == nil {
		return nil
	}

	ret := make([]*mvccpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		newKV := toKV(kv)
		if newKV != nil {
			ret = append(ret, newKV)
		}
	}
	return ret
}

func toKV(kv *KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		Lease:          kv.Lease,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
	}
}
