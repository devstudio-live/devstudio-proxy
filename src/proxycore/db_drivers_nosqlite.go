//go:build !sqlite
// +build !sqlite

package proxycore

func sqliteAvailable() bool {
	return false
}
