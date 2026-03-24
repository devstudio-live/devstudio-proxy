//go:build !sqlite
// +build !sqlite

package main

// sqliteAvailable returns false when sqlite support is not compiled in
func sqliteAvailable() bool {
	return false
}
