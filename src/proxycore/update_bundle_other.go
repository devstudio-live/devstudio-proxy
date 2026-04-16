//go:build !darwin

package proxycore

import (
	"context"
	"fmt"
)

func applyBundleUpdate(_ context.Context, _ string) error {
	return fmt.Errorf(".app bundle update is only supported on macOS")
}

func rollbackBundleUpdate() {}
