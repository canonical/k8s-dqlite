module github.com/canonical/k8s-dqlite

go 1.16

require (
	github.com/canonical/go-dqlite v1.8.1-0.20210812152211-7289bce1c8aa
	github.com/ghodss/yaml v1.0.0
	github.com/pkg/errors v0.9.1
	github.com/rancher/kine v0.4.1
	github.com/sirupsen/logrus v1.7.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	golang.org/x/sys v0.0.0-20210603081109-ebe580a85c40
	k8s.io/component-base v0.18.0
)

replace github.com/rancher/kine => github.com/canonical/kine v0.4.1-k8s-dqlite.1
