package sqlite_test

import (
	"fmt"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
)

func TestCanMigrate(t *testing.T) {
	tests := []struct {
		name        string
		current     sqlite.SchemaVersion
		target      sqlite.SchemaVersion
		canMigrate  bool
		expectedErr error
	}{
		{
			name:        "can not migrate between different major versions",
			current:     sqlite.ToSchemaVersion(0, 0),
			target:      sqlite.ToSchemaVersion(1, 0),
			canMigrate:  false,
			expectedErr: fmt.Errorf("can not migrate between different major versions"),
		},
		{
			name:        "can not migrate between same versions",
			current:     sqlite.ToSchemaVersion(0, 0),
			target:      sqlite.ToSchemaVersion(0, 0),
			canMigrate:  false,
			expectedErr: nil,
		},
		{
			name:        "can not rollback minor version",
			current:     sqlite.ToSchemaVersion(1, 1),
			target:      sqlite.ToSchemaVersion(1, 0),
			canMigrate:  false,
			expectedErr: fmt.Errorf("can not rollback to earlier minor version"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canMigrate, err := tt.current.CanMigrate(tt.target)
			if canMigrate != tt.canMigrate {
				t.Errorf("expected canMigrate %v, got %v", tt.canMigrate, canMigrate)
			}
			if err != nil && tt.expectedErr == nil {
				if err.Error() != tt.expectedErr.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedErr, err)
				}
			}
		})
	}
}
