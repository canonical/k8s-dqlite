package dbctl

import (
	"context"
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	flagPeerURL string

	memberCmd = &cobra.Command{
		Use:   "member",
		Short: "Manage cluster members",
	}

	memberListCmd = &cobra.Command{
		Use:          "list",
		Short:        "List cluster members",
		SilenceUsage: true,
		RunE: command(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			return client.MemberList(ctx)
		}),
	}

	memberAddCmd = &cobra.Command{
		Use:          "add [peerURL ...]",
		Short:        "Add a new cluster member",
		SilenceUsage: true,
		Args:         cobra.MinimumNArgs(1),
		RunE: command(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			return client.MemberAdd(ctx, args)
		}),
	}

	memberRemoveCmd = &cobra.Command{
		Use:          "remove [memberID]",
		Short:        "Remove a cluster member",
		SilenceUsage: true,
		Args:         cobra.MaximumNArgs(1),
		RunE: command(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			switch {
			case flagPeerURL == "" && len(args) == 0:
				return nil, fmt.Errorf("specify member ID as argument, or --peer-url flag")
			case flagPeerURL != "" && len(args) == 1:
				return nil, fmt.Errorf("--peer-url flag not allowed when a member ID is specified")
			case len(args) == 1:
				id, err := strconv.ParseUint(args[0], 10, 64)
				if err != nil {
					return nil, fmt.Errorf("cannot parse member ID %q: %w", args[0], err)
				}
				return client.MemberRemove(ctx, id)
			default:
				members, err := client.MemberList(ctx)
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve list of cluster members: %w", err)
				}
				for _, member := range members.Members {
					for _, url := range member.PeerURLs {
						if url == flagPeerURL {
							return client.MemberRemove(ctx, member.ID)
						}
					}
				}
				return nil, fmt.Errorf("cluster member not found")
			}
		}),
	}
)

func init() {
	// member list
	memberCmd.AddCommand(memberListCmd)

	// member add
	memberCmd.AddCommand(memberAddCmd)

	// member remove
	memberRemoveCmd.Flags().StringVar(&flagPeerURL, "peer-url", "", "remove cluster member with a matching peer URL")
	memberCmd.AddCommand(memberRemoveCmd)
}
