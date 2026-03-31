package proxycore

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// fsRequest is the unified request body for all filesystem gateway endpoints.
type fsRequest struct {
	Path     string `json:"path"`
	NewPath  string `json:"newPath,omitempty"`  // rename/move
	Content  string `json:"content,omitempty"`  // write (utf8 or base64)
	IsBase64 bool   `json:"isBase64,omitempty"` // true = content is base64-encoded
	Offset   *int64 `json:"offset,omitempty"`   // read: byte offset (nil = 0)
	Length   *int64 `json:"length,omitempty"`   // read: byte count (nil = entire file)
	Append   bool   `json:"append,omitempty"`   // write: append instead of overwrite
}

// fsEntry describes a file or directory.
type fsEntry struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	IsDir   bool   `json:"isDir"`
	Size    int64  `json:"size"`
	ModTime string `json:"modTime"` // ISO 8601
	Mode    string `json:"mode"`    // "drwxr-xr-x"
}

// fsResponse is the unified response body for all filesystem gateway endpoints.
type fsResponse struct {
	Entries   []fsEntry `json:"entries,omitempty"`
	Entry     *fsEntry  `json:"entry,omitempty"`
	Content   string    `json:"content,omitempty"`
	Encoding  string    `json:"encoding,omitempty"`  // "utf8" | "base64"
	TotalSize int64     `json:"totalSize,omitempty"` // read: full file size (for progress)
	Error     string    `json:"error,omitempty"`
}

// handleFSGateway is the entry-point for all filesystem gateway requests.
// It strips the gateway routing headers (defense-in-depth) and dispatches
// to the appropriate sub-handler based on r.URL.Path.
func handleFSGateway(w http.ResponseWriter, r *http.Request) {
	// Strip gateway routing headers so they are never forwarded upstream.
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	// Only POST is accepted.
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(fsResponse{Error: "only POST is accepted"})
		return
	}

	var req fsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(fsResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	// Sanitize the path early.
	cleanPath, err := sanitizePath(req.Path)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(fsResponse{Error: "invalid path: " + err.Error()})
		return
	}
	req.Path = cleanPath

	// Sanitize newPath if present.
	if req.NewPath != "" {
		cleanNewPath, err := sanitizePath(req.NewPath)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(fsResponse{Error: "invalid newPath: " + err.Error()})
			return
		}
		req.NewPath = cleanNewPath
	}

	// Dispatch by path (path is flexible since routing is header-based).
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "list":
		handleFSList(w, r, req)
	case "stat":
		handleFSStat(w, r, req)
	case "read":
		handleFSRead(w, r, req)
	case "write":
		handleFSWrite(w, r, req)
	case "mkdir":
		handleFSMkdir(w, r, req)
	case "delete":
		handleFSDelete(w, r, req)
	case "rename":
		handleFSRename(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(fsResponse{Error: "unknown gateway endpoint: " + path})
	}
}

// handleFSList lists the contents of a directory.
func handleFSList(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}

	entries, err := os.ReadDir(req.Path)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to list directory: " + err.Error()})
		return
	}

	result := make([]fsEntry, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue // skip entries with stat errors
		}

		result = append(result, fsEntry{
			Name:    entry.Name(),
			Path:    filepath.Join(req.Path, entry.Name()),
			IsDir:   entry.IsDir(),
			Size:    info.Size(),
			ModTime: info.ModTime().UTC().Format(time.RFC3339),
			Mode:    info.Mode().String(),
		})
	}

	json.NewEncoder(w).Encode(fsResponse{Entries: result})
}

// handleFSStat returns metadata for a file or directory.
func handleFSStat(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}

	info, err := os.Stat(req.Path)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to stat path: " + err.Error()})
		return
	}

	entry := fsEntry{
		Name:    info.Name(),
		Path:    req.Path,
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Mode:    info.Mode().String(),
	}

	json.NewEncoder(w).Encode(fsResponse{Entry: &entry})
}

// handleFSRead reads a file or a chunk of a file.
func handleFSRead(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}

	file, err := os.Open(req.Path)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to open file: " + err.Error()})
		return
	}
	defer file.Close()

	// Get file size for progress tracking.
	info, err := file.Stat()
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to stat file: " + err.Error()})
		return
	}
	totalSize := info.Size()

	// Determine offset and length.
	var offset int64 = 0
	if req.Offset != nil {
		offset = *req.Offset
	}

	var length int64 = totalSize - offset
	if req.Length != nil && *req.Length >= 0 {
		if *req.Length < length {
			length = *req.Length
		}
	}

	// Seek to offset.
	if offset > 0 {
		if _, err := file.Seek(offset, 0); err != nil {
			json.NewEncoder(w).Encode(fsResponse{Error: "failed to seek file: " + err.Error()})
			return
		}
	}

	// Read the requested chunk.
	data := make([]byte, length)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.ErrUnexpectedEOF {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to read file: " + err.Error()})
		return
	}
	data = data[:n]

	// Auto-detect binary vs. text via content-type detection.
	detectedType := http.DetectContentType(data)
	encoding := "utf8"
	content := string(data)

	// If binary, encode as base64.
	if !strings.HasPrefix(detectedType, "text/") && detectedType != "application/json" && detectedType != "application/xml" {
		encoding = "base64"
		content = base64.StdEncoding.EncodeToString(data)
	}

	json.NewEncoder(w).Encode(fsResponse{
		Content:   content,
		Encoding:  encoding,
		TotalSize: totalSize,
	})
}

// handleFSWrite writes to a file, with optional append mode for chunked writes.
func handleFSWrite(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}
	if req.Content == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "content is required"})
		return
	}

	// Decode content from base64 if needed.
	var data []byte
	var err error
	if req.IsBase64 {
		data, err = base64.StdEncoding.DecodeString(req.Content)
		if err != nil {
			json.NewEncoder(w).Encode(fsResponse{Error: "failed to decode base64: " + err.Error()})
			return
		}
	} else {
		data = []byte(req.Content)
	}

	// Open file with appropriate flags.
	var flags int
	if req.Append {
		flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	} else {
		flags = os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	}

	file, err := os.OpenFile(req.Path, flags, 0644)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to open file for writing: " + err.Error()})
		return
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to write file: " + err.Error()})
		return
	}

	// Return updated file metadata.
	info, err := os.Stat(req.Path)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to stat file after write: " + err.Error()})
		return
	}

	entry := fsEntry{
		Name:    info.Name(),
		Path:    req.Path,
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Mode:    info.Mode().String(),
	}

	json.NewEncoder(w).Encode(fsResponse{Entry: &entry})
}

// handleFSMkdir creates a directory.
func handleFSMkdir(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}

	if err := os.MkdirAll(req.Path, 0755); err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to create directory: " + err.Error()})
		return
	}

	// Return directory metadata.
	info, err := os.Stat(req.Path)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to stat directory after creation: " + err.Error()})
		return
	}

	entry := fsEntry{
		Name:    info.Name(),
		Path:    req.Path,
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Mode:    info.Mode().String(),
	}

	json.NewEncoder(w).Encode(fsResponse{Entry: &entry})
}

// handleFSDelete deletes a file or directory recursively.
func handleFSDelete(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path is required"})
		return
	}

	if err := os.RemoveAll(req.Path); err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to delete path: " + err.Error()})
		return
	}

	json.NewEncoder(w).Encode(fsResponse{})
}

// handleFSRename renames or moves a file or directory.
func handleFSRename(w http.ResponseWriter, r *http.Request, req fsRequest) {
	if req.Path == "" || req.NewPath == "" {
		json.NewEncoder(w).Encode(fsResponse{Error: "path and newPath are required"})
		return
	}

	if err := os.Rename(req.Path, req.NewPath); err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to rename: " + err.Error()})
		return
	}

	// Return updated metadata.
	info, err := os.Stat(req.NewPath)
	if err != nil {
		json.NewEncoder(w).Encode(fsResponse{Error: "failed to stat file after rename: " + err.Error()})
		return
	}

	entry := fsEntry{
		Name:    info.Name(),
		Path:    req.NewPath,
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime().UTC().Format(time.RFC3339),
		Mode:    info.Mode().String(),
	}

	json.NewEncoder(w).Encode(fsResponse{Entry: &entry})
}

// sanitizePath cleans and validates a file path.
// It uses filepath.Clean and filepath.EvalSymlinks and rejects null bytes.
func sanitizePath(path string) (string, error) {
	// Reject null bytes.
	if strings.Contains(path, "\x00") {
		return "", ErrInvalidPath
	}

	// Handle home directory expansion.
	if strings.HasPrefix(path, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[1:])
	}

	// Clean the path.
	cleanPath := filepath.Clean(path)

	// Evaluate symlinks to prevent directory traversal attacks.
	realPath, err := filepath.EvalSymlinks(cleanPath)
	if err != nil {
		// If EvalSymlinks fails (e.g., path doesn't exist yet), use the cleaned path.
		// This is OK for creating new files/directories.
		realPath = cleanPath
	}

	return realPath, nil
}

// ErrInvalidPath is returned when a path contains invalid characters.
var ErrInvalidPath = &pathError{msg: "invalid path: contains null bytes"}

type pathError struct {
	msg string
}

func (e *pathError) Error() string {
	return e.msg
}
