module github.com/canonical/k8s-dqlite

go 1.16

require (
	github.com/canonical/go-dqlite v1.10.1-0.20211018012438-4123028456df
	github.com/ghodss/yaml v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.4.1
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22
	k8s.io/component-base v0.18.0
)

replace github.com/rancher/kine => github.com/canonical/kine v0.4.1-k8s-dqlite.1
