module github.com/canonical/k8s-dqlite

go 1.16

require (
	github.com/canonical/kvsql-dqlite v0.0.0-20210513073226-3dab903dba87
	github.com/sirupsen/logrus v1.6.0
	github.com/spf13/cobra v1.0.0
	github.com/spf13/pflag v1.0.5
	golang.org/x/sys v0.0.0-20200622214017-ed371f2e16b4
	k8s.io/component-base v0.17.0
)

replace (
	github.com/canonical/go-dqlite => github.com/canonical/go-dqlite v1.8.0
	// github.com/canonical/kvsql-dqlite => /home/jackal/go/src/github.com/canonical/kvsql-dqlite
	github.com/k3s-io/kine => github.com/canonical/kine v0.4.1-0.20210514110539-195ffcf66b2f
	//github.com/k3s-io/kine => /home/jackal/go/src/github.com/k3s-io/kine
)
