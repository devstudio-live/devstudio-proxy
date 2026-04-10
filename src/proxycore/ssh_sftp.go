package proxycore

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/pkg/sftp"
)

// SSHFileInfo is the JSON representation of a remote file.
type SSHFileInfo struct {
	Name    string `json:"name"`
	Size    int64  `json:"size"`
	Mode    string `json:"mode"`
	ModTime string `json:"modTime"`
	IsDir   bool   `json:"isDir"`
}

// sftpClientFor opens an SFTP client on an already-pooled SSH connection.
// If the pooled connection is stale (e.g. server rejected channel open),
// it evicts the entry and retries once with a fresh connection.
func (s *Server) sftpClientFor(conn SSHConnection) (*sftp.Client, error) {
	sshClient, err := s.getPooledSSHClient(conn)
	if err != nil {
		return nil, fmt.Errorf("SSH: %w", err)
	}
	sc, err := sftp.NewClient(sshClient)
	if err == nil {
		return sc, nil
	}
	// "open failed" means the server rejected the session channel — the pooled
	// connection may be exhausted or stale.  Evict it and dial a fresh one.
	s.closeSSHConnection(sshConnectionKey(conn))
	sshClient2, err2 := s.getPooledSSHClient(conn)
	if err2 != nil {
		return nil, fmt.Errorf("SSH (retry): %w", err2)
	}
	sc2, err2 := sftp.NewClient(sshClient2)
	if err2 != nil {
		return nil, fmt.Errorf("SFTP client: %w", err2)
	}
	return sc2, nil
}

// ── Gateway handlers ──────────────────────────────────────────────────────────

func (s *Server) handleSFTPList(w http.ResponseWriter, req SSHRequest, start time.Time) {
	dirPath := req.SFTPPath
	if dirPath == "" {
		dirPath = "."
	}

	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	entries, err := sc.ReadDir(dirPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "readdir: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	files := make([]map[string]any, 0, len(entries))
	for _, fi := range entries {
		files = append(files, map[string]any{
			"name":    fi.Name(),
			"size":    fi.Size(),
			"mode":    fi.Mode().String(),
			"modTime": fi.ModTime().UTC().Format(time.RFC3339),
			"isDir":   fi.IsDir(),
		})
	}

	// Resolve to absolute path for the response
	absPath, err := sc.RealPath(dirPath)
	if err != nil {
		absPath = dirPath
	}

	json.NewEncoder(w).Encode(SSHResponse{ //nolint:errcheck
		Success:    true,
		DurationMs: ms(start),
		Item:       map[string]any{"path": absPath},
		Files:      &files,
	})
}

func (s *Server) handleSFTPStat(w http.ResponseWriter, req SSHRequest, start time.Time) {
	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	fi, err := sc.Stat(req.SFTPPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "stat: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	json.NewEncoder(w).Encode(SSHResponse{ //nolint:errcheck
		Success:    true,
		DurationMs: ms(start),
		Item: map[string]any{
			"name":    fi.Name(),
			"size":    fi.Size(),
			"mode":    fi.Mode().String(),
			"modTime": fi.ModTime().UTC().Format(time.RFC3339),
			"isDir":   fi.IsDir(),
		},
	})
}

// handleSFTPRead reads a file and returns its content base64-encoded.
// Files larger than 50 MB are rejected to avoid OOM.
func (s *Server) handleSFTPRead(w http.ResponseWriter, req SSHRequest, start time.Time) {
	const maxBytes = 50 << 20 // 50 MB

	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	fi, err := sc.Stat(req.SFTPPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "stat: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	if fi.Size() > maxBytes {
		json.NewEncoder(w).Encode(SSHResponse{Error: fmt.Sprintf("file too large (%d bytes): use download endpoint", fi.Size()), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	f, err := sc.Open(req.SFTPPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "open: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxBytes))
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "read: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	json.NewEncoder(w).Encode(SSHResponse{ //nolint:errcheck
		Success:    true,
		DurationMs: ms(start),
		Item: map[string]any{
			"name":    fi.Name(),
			"size":    fi.Size(),
			"content": base64.StdEncoding.EncodeToString(data),
		},
	})
}

// handleSFTPDownload streams a remote file directly as an HTTP response.
// The path and connection come from query params (GET request).
func (s *Server) handleSFTPDownload(w http.ResponseWriter, r *http.Request, start time.Time) {
	// Decode the request body which still carries connection + path
	var req SSHRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer sc.Close()

	fi, err := sc.Stat(req.SFTPPath)
	if err != nil {
		http.Error(w, "stat: "+err.Error(), http.StatusNotFound)
		return
	}

	f, err := sc.Open(req.SFTPPath)
	if err != nil {
		http.Error(w, "open: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, fi.Name()))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", fi.Size()))
	io.Copy(w, f) //nolint:errcheck
}

func (s *Server) handleSFTPWrite(w http.ResponseWriter, r *http.Request, req SSHRequest, start time.Time) {
	// Multipart: field "file" carries the file data, "path" overrides SFTPPath
	if err := r.ParseMultipartForm(200 << 20); err != nil {
		// Fallback: base64-encoded content in JSON body field
		if req.SFTPContent == "" {
			json.NewEncoder(w).Encode(SSHResponse{Error: "parse upload: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
			return
		}
	}

	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	// Base64 content path (small files, from JSON body)
	if req.SFTPContent != "" {
		data, err := base64.StdEncoding.DecodeString(req.SFTPContent)
		if err != nil {
			json.NewEncoder(w).Encode(SSHResponse{Error: "decode content: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
			return
		}
		if err := sftpWriteBytes(sc, req.SFTPPath, data); err != nil {
			json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
			return
		}
		json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "uploaded", DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	// Multipart path
	mf, _, err := r.FormFile("file")
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "form file: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer mf.Close()

	destPath := req.SFTPPath
	if destPath == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "sftp path is required", DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	dst, err := sc.Create(destPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "create remote file: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer dst.Close()

	if _, err := io.Copy(dst, mf); err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "write: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "uploaded", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSFTPMkdir(w http.ResponseWriter, req SSHRequest, start time.Time) {
	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	if err := sc.MkdirAll(req.SFTPPath); err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "mkdir: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "directory created", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSFTPDelete(w http.ResponseWriter, req SSHRequest, start time.Time) {
	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	// Try RemoveDirectory first (works for empty dirs), then Remove for files
	fi, err := sc.Stat(req.SFTPPath)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "stat: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	if fi.IsDir() {
		err = sc.RemoveDirectory(req.SFTPPath)
	} else {
		err = sc.Remove(req.SFTPPath)
	}
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "delete: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "deleted", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSFTPRename(w http.ResponseWriter, req SSHRequest, start time.Time) {
	if req.SFTPDest == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "sftpDest is required", DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	sc, err := s.sftpClientFor(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sc.Close()

	// PosixRename atomically replaces the destination (like POSIX rename(2)),
	// avoiding SSH_FX_FAILURE when the destination already exists.
	if err := sc.PosixRename(req.SFTPPath, req.SFTPDest); err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "rename: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "renamed", DurationMs: ms(start)}) //nolint:errcheck
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func sftpWriteBytes(sc *sftp.Client, remotePath string, data []byte) error {
	// Ensure parent directory exists
	if err := sc.MkdirAll(path.Dir(remotePath)); err != nil {
		return fmt.Errorf("mkdir parent: %w", err)
	}
	f, err := sc.Create(remotePath)
	if err != nil {
		return fmt.Errorf("create: %w", err)
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}
