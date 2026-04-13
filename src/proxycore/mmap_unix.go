//go:build !windows

package proxycore

import "syscall"

func mmapFile(fd int, size int) ([]byte, error) {
	return syscall.Mmap(fd, 0, size, syscall.PROT_READ, syscall.MAP_PRIVATE)
}

func munmapFile(data []byte) error {
	return syscall.Munmap(data)
}
