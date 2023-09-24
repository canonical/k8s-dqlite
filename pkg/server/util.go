package server

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v2"
)

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
		if !os.IsNotExist(err) {
			return false, fmt.Errorf("failed to stat: %w", err)
		}
		return false, nil
	}
	return true, nil
}

func IsStorageFull(storageDir string, diskThreshold uint64) (bool, error) {
	var stat unix.Statfs_t
	err := unix.Statfs(storageDir, &stat)
	if err != nil {
		return false, fmt.Errorf("failed to check disk capacity: %w", err)
	}
	avail := stat.Bavail * uint64(stat.Bsize)
	// If available space is less than 10MB reject to start.
	return avail < diskThreshold, nil
}
