package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/client"
)

func putKey(ctx context.Context, c client.Client, key string, value []byte) error {
	err := c.Create(ctx, key, value)
	if err == nil {
		return nil
	} else if !errors.Is(err, client.ErrKeyExists) {
		return fmt.Errorf("failed to create key %q: %w", key, err)
	}
	// failed to create key because it exists, make a few attempts to overwrite it
	for i := 0; i < 5 && err != nil; i++ {
		time.Sleep(50 * time.Millisecond)
		err = c.Put(ctx, key, value)

		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("failed to put key %q: %w", key, err)
}
