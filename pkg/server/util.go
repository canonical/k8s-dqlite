package server

import (
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite/v3/app"
	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
)

// busyTimeoutOption returns the dqlite option that makes a write wait up to
// timeout for a concurrent write transaction to finish, instead of failing
// immediately with SQLITE_BUSY ("database is locked"). A zero timeout keeps
// dqlite's fail-fast default and returns a nil option. dqlite takes the
// timeout as unsigned milliseconds, so sub-millisecond precision is dropped.
func busyTimeoutOption(timeout time.Duration) (app.Option, error) {
	if timeout < 0 {
		return nil, fmt.Errorf("invalid busy timeout %v: must not be negative", timeout)
	}
	if timeout.Milliseconds() > math.MaxUint32 {
		return nil, fmt.Errorf("invalid busy timeout %v: must not exceed %v", timeout, time.Duration(math.MaxUint32)*time.Millisecond)
	}
	if timeout.Milliseconds() == 0 {
		return nil, nil
	}
	return app.WithBusyTimeout(timeout), nil
}

func fileUnmarshal(v interface{}, path ...string) error {
	b, err := os.ReadFile(filepath.Join(path...))
	if err != nil {
		return fmt.Errorf("failed to read file contents: %w", err)
	}
	if err := yaml.Unmarshal(b, v); err != nil {
		return fmt.Errorf("failed to parse as yaml: %w", err)
	}
	return nil
}

func fileMarshal(v interface{}, path ...string) error {
	b, err := yaml.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal yaml: %w", err)
	}
	if err := os.WriteFile(filepath.Join(path...), b, 0600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

func fileExists(path ...string) (bool, error) {
	if _, err := os.Stat(filepath.Join(path...)); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return false, fmt.Errorf("failed to stat: %w", err)
		}
		return false, nil
	}
	return true, nil
}

func checkAvailableStorageSize(storageDir string, minimumBytes uint64) error {
	var stat unix.Statfs_t
	if err := unix.Statfs(storageDir, &stat); err != nil {
		return fmt.Errorf("failed to check available disk size: %w", err)
	}
	if availableBytes := stat.Bavail * uint64(stat.Bsize); availableBytes < minimumBytes {
		return fmt.Errorf("available disk size (%v bytes) is below minimum required (%v bytes)", availableBytes, minimumBytes)
	}
	return nil
}
