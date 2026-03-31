//go:build sqlite
// +build sqlite

package proxycore

import (
	_ "modernc.org/sqlite"
)

func sqliteAvailable() bool {
	return true
}
