package embeddedctl

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	flagPeerURLs []string
	flagMemberID uint64

	memberCmd = &cobra.Command{
		Use: "member",
	}

	memberListCmd = &cobra.Command{
		Use: "list",
		RunE: newCommand(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			return client.MemberList(ctx)
		}),
	}

	memberAddCmd = &cobra.Command{
		Use:  "add",
		Args: cobra.MinimumNArgs(1),
		RunE: newCommand(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			return client.MemberAdd(ctx, args)
		}),
	}

	memberRemoveCmd = &cobra.Command{
		Use:  "add",
		Args: cobra.ExactArgs(1),
		RunE: newCommand(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("cannot parse member ID %q: %w", args[0], err)
			}
			return client.MemberRemove(ctx, id)
		}),
	}
)

func init() {
	memberCmd.AddCommand(memberListCmd)
	memberCmd.AddCommand(memberAddCmd)
	memberCmd.AddCommand(memberRemoveCmd)
}
