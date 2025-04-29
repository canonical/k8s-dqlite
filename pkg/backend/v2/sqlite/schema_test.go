package sqlite_test

import (
	"fmt"
	"testing"

	sqlite "github.com/canonical/k8s-dqlite/pkg/backend/v1/sqlite"
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
			current:     sqlite.NewSchemaVersion(0, 0),
			target:      sqlite.NewSchemaVersion(1, 0),
			expectedErr: fmt.Errorf("can not migrate between different major versions"),
		},
		{
			name:        "can migrate",
			current:     sqlite.NewSchemaVersion(0, 0),
			target:      sqlite.NewSchemaVersion(0, 1),
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.current.CompatibleWith(tt.target)
			if err != nil && tt.expectedErr == nil {
				if err.Error() != tt.expectedErr.Error() {
					t.Errorf("expected error %v, got %v", tt.expectedErr, err)
				}
			}
		})
	}
}
