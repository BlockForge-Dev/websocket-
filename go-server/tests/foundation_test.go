package tests

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMilestoneZeroRepositoryFilesExist(t *testing.T) {
	t.Parallel()

	required := []string{
		"../go.mod",
		"../README.md",
		"../CONTRIBUTING.md",
		"../Makefile",
		"../docs/architecture.md",
		"../docs/message-protocol.md",
		"../docs/version-one-scope.md",
	}

	for _, name := range required {
		name := name
		t.Run(filepath.Base(name), func(t *testing.T) {
			t.Parallel()
			if _, err := os.Stat(name); err != nil {
				t.Fatalf("required repository file %q is unavailable: %v", name, err)
			}
		})
	}
}
