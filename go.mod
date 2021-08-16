module github.com/canonical/k8s-dqlite

go 1.16

require (
	github.com/PuerkitoBio/goquery v1.5.1 // indirect
	github.com/Rican7/retry v0.1.0 // indirect
	github.com/canonical/go-dqlite v1.7.0
	github.com/coreos/etcd v3.3.22+incompatible // indirect
	github.com/emicklei/go-restful v2.13.0+incompatible // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-sql-driver/mysql v1.5.0 // indirect
	github.com/gogo/protobuf v1.3.1 // indirect
	github.com/golang/protobuf v1.4.2 // indirect
	github.com/json-iterator/go v1.1.10 // indirect
	github.com/k3s-io/kine v0.0.0-00010101000000-000000000000
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.4.0 // indirect
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.0
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0 // indirect
	golang.org/x/net v0.0.0-20200602114024-627f9648deb9 // indirect
	golang.org/x/sys v0.0.0-20200622214017-ed371f2e16b4
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/appengine v1.6.6 // indirect
	google.golang.org/grpc/examples v0.0.0-20210211230339-9f3606cd0f76 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/yaml.v2 v2.3.0 // indirect
	k8s.io/apimachinery v0.17.0 // indirect
	k8s.io/apiserver v0.17.0 // indirect
	k8s.io/component-base v0.17.0
)

replace (
	github.com/canonical/go-dqlite => github.com/canonical/go-dqlite v1.8.0
	github.com/k3s-io/kine => github.com/canonical/kine v0.4.1-0.20210521130757-0b3ec91dccd6
//github.com/k3s-io/kine => /home/jackal/go/src/github.com/k3s-io/kine
)
