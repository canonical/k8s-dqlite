module github.com/canonical/k8s-dqlite

go 1.16

require (
	github.com/canonical/go-dqlite v1.11.6
	github.com/ghodss/yaml v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.4.1
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cobra v1.2.1
	github.com/spf13/pflag v1.0.5
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22
	gopkg.in/yaml.v2 v2.4.0
	k8s.io/component-base v0.18.0
)

replace github.com/rancher/kine => github.com/marco6/kine v0.0.0-20230228103807-5edb9a2509cd
