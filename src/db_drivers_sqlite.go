//go:build sqlite
// +build sqlite

package main

import (
	_ "modernc.org/sqlite"
)

// sqliteAvailable returns true when sqlite support is compiled in
func sqliteAvailable() bool {
	return true
}
