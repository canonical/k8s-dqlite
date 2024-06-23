package etcd

import (
	"context"
	"fmt"
)

func (e *etcd) Shutdown(ctx context.Context) error {
	if e.instance == nil {
		return nil
	}
	e.instance.Close()

	select {
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for server to stop: %w", ctx.Err())
	case <-e.instance.Server.StopNotify():
	}

	close(e.mustStopCh)
	return nil
}
