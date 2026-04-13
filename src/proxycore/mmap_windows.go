//go:build windows

package proxycore

import "os"

func mmapFile(fd int, size int) ([]byte, error) {
	// Windows fallback: read the entire file into memory instead of mmap.
	// The fd is not directly usable here, so we re-read via /proc/self/fd is
	// not available on Windows. The caller must use mmapFileFromPath instead.
	// This path is unused — see mmapFileFromPath below.
	buf := make([]byte, size)
	f := os.NewFile(uintptr(fd), "")
	if f == nil {
		return nil, os.ErrInvalid
	}
	_, err := f.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func munmapFile(data []byte) error {
	// No-op: data was allocated with make(), GC will collect it.
	return nil
}
