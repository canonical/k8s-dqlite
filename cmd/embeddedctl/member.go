package embeddedctl

import (
	"context"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var memberCmd = &cobra.Command{
	Use: "member",
}

var memberListCmd = &cobra.Command{
	Use: "list",
	RunE: newCommand(func(ctx context.Context, client *clientv3.Client) (any, error) {
		return client.MemberList(ctx)
	}),
}

func init() {
	memberCmd.AddCommand(memberListCmd)
}
