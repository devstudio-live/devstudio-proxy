package proxycore

import (
	"fmt"

	"github.com/pkg/sftp"
)

// SFTPDownloadHandle wraps an open remote file for streaming reads. Close
// must be called to release both the file and the underlying SFTP client.
type SFTPDownloadHandle struct {
	Name string
	Size int64

	file *sftp.File
	sc   *sftp.Client
}

// Read forwards to the underlying sftp.File.
func (h *SFTPDownloadHandle) Read(p []byte) (int, error) {
	return h.file.Read(p)
}

// Close releases the file handle and the SFTP client (the pooled SSH client
// itself remains alive).
func (h *SFTPDownloadHandle) Close() error {
	ferr := h.file.Close()
	serr := h.sc.Close()
	if ferr != nil {
		return ferr
	}
	return serr
}

// OpenSFTPDownloadStream opens a remote file for streaming reads. The caller
// is responsible for closing the returned handle. Returns an error if the
// target is a directory or cannot be opened.
func (s *Server) OpenSFTPDownloadStream(conn SSHConnection, remotePath string) (*SFTPDownloadHandle, error) {
	sc, err := s.sftpClientFor(conn)
	if err != nil {
		return nil, err
	}
	fi, err := sc.Stat(remotePath)
	if err != nil {
		sc.Close()
		return nil, fmt.Errorf("stat: %w", err)
	}
	if fi.IsDir() {
		sc.Close()
		return nil, fmt.Errorf("%s is a directory", remotePath)
	}
	f, err := sc.Open(remotePath)
	if err != nil {
		sc.Close()
		return nil, fmt.Errorf("open: %w", err)
	}
	return &SFTPDownloadHandle{
		Name: fi.Name(),
		Size: fi.Size(),
		file: f,
		sc:   sc,
	}, nil
}

// SFTPUploadHandle wraps an open remote file for streaming writes. Close must
// be called to release both the file and the underlying SFTP client.
type SFTPUploadHandle struct {
	file *sftp.File
	sc   *sftp.Client
}

// Write forwards to the underlying sftp.File.
func (h *SFTPUploadHandle) Write(p []byte) (int, error) {
	return h.file.Write(p)
}

// Close releases the file handle and the SFTP client.
func (h *SFTPUploadHandle) Close() error {
	ferr := h.file.Close()
	serr := h.sc.Close()
	if ferr != nil {
		return ferr
	}
	return serr
}

// OpenSFTPUploadStream creates (or truncates) a remote file for streaming
// writes. The caller is responsible for closing the returned handle.
func (s *Server) OpenSFTPUploadStream(conn SSHConnection, remotePath string) (*SFTPUploadHandle, error) {
	sc, err := s.sftpClientFor(conn)
	if err != nil {
		return nil, err
	}
	f, err := sc.Create(remotePath)
	if err != nil {
		sc.Close()
		return nil, fmt.Errorf("create: %w", err)
	}
	return &SFTPUploadHandle{file: f, sc: sc}, nil
}
