package main

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"sync"

	"devstudio/proxy/proxycore"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// sftpDownloadState is the per-session state for a chunked SFTP download.
type sftpDownloadState struct {
	cancel chan struct{}
	once   sync.Once
}

func (s *sftpDownloadState) stop() {
	s.once.Do(func() { close(s.cancel) })
}

const sftpDownloadChunkBytes = 256 * 1024

// SFTPDownloadStart kicks off a chunked SFTP download. The frontend is
// responsible for generating sessionID and subscribing to the event stream
// BEFORE calling this method:
//
//	sftp-download-meta-{id}     one-off  {"name","size","contentType"}
//	sftp-download-chunk-{id}    repeated base64 chunk
//	sftp-download-progress-{id} repeated running byte total
//	sftp-download-done-{id}     one-off  (success)
//	sftp-download-error-{id}    one-off  error message
//
// Returns {"ok":true} immediately once the goroutine is spawned; the SFTP
// open + streaming runs in the background.
func (a *App) SFTPDownloadStart(sessionID string, connJSON string, sftpPath string) string {
	if sessionID == "" {
		return sshErrJSON("sessionId required")
	}
	var conn proxycore.SSHConnection
	if err := json.Unmarshal([]byte(connJSON), &conn); err != nil {
		return sshErrJSON(err.Error())
	}

	state := &sftpDownloadState{cancel: make(chan struct{})}
	a.sftpDownloads.Store(sessionID, state)

	go func() {
		defer a.sftpDownloads.Delete(sessionID)

		handle, err := a.proxy.OpenSFTPDownloadStream(conn, sftpPath)
		if err != nil {
			runtime.EventsEmit(a.ctx, "sftp-download-error-"+sessionID, err.Error())
			return
		}
		defer handle.Close()

		meta, _ := json.Marshal(map[string]any{
			"name":        handle.Name,
			"size":        handle.Size,
			"contentType": "application/octet-stream",
		})
		runtime.EventsEmit(a.ctx, "sftp-download-meta-"+sessionID, string(meta))

		buf := make([]byte, sftpDownloadChunkBytes)
		var total int64
		for {
			select {
			case <-state.cancel:
				runtime.EventsEmit(a.ctx, "sftp-download-error-"+sessionID, "cancelled")
				return
			default:
			}

			n, rerr := handle.Read(buf)
			if n > 0 {
				runtime.EventsEmit(a.ctx, "sftp-download-chunk-"+sessionID, base64.StdEncoding.EncodeToString(buf[:n]))
				total += int64(n)
				runtime.EventsEmit(a.ctx, "sftp-download-progress-"+sessionID, total)
			}
			if rerr == io.EOF {
				runtime.EventsEmit(a.ctx, "sftp-download-done-"+sessionID, "")
				return
			}
			if rerr != nil {
				runtime.EventsEmit(a.ctx, "sftp-download-error-"+sessionID, rerr.Error())
				return
			}
		}
	}()

	return `{"ok":true}`
}

// SFTPDownloadCancel stops an in-progress download. Safe to call after the
// download has already finished.
func (a *App) SFTPDownloadCancel(sessionID string) string {
	v, ok := a.sftpDownloads.LoadAndDelete(sessionID)
	if !ok {
		return `{"ok":true}`
	}
	v.(*sftpDownloadState).stop()
	return `{"ok":true}`
}

// ── SFTP upload (chunked) ────────────────────────────────────────────────────

// sftpUploadState owns an open remote file for the duration of an upload.
// Concurrent Chunk calls for the same sessionID are serialized via mu; the
// frontend is expected to issue chunks sequentially (awaiting each call).
type sftpUploadState struct {
	handle *proxycore.SFTPUploadHandle
	mu     sync.Mutex
	closed bool
}

// SFTPUploadStart opens a remote file for streaming writes. The frontend
// generates sessionID, calls Start, then pumps bytes via SFTPUploadChunk,
// and finalises with SFTPUploadFinish (or SFTPUploadCancel on failure).
func (a *App) SFTPUploadStart(sessionID string, connJSON string, sftpPath string) string {
	if sessionID == "" {
		return sshErrJSON("sessionId required")
	}
	var conn proxycore.SSHConnection
	if err := json.Unmarshal([]byte(connJSON), &conn); err != nil {
		return sshErrJSON(err.Error())
	}
	handle, err := a.proxy.OpenSFTPUploadStream(conn, sftpPath)
	if err != nil {
		return sshErrJSON(err.Error())
	}
	a.sftpUploads.Store(sessionID, &sftpUploadState{handle: handle})
	return `{"ok":true}`
}

// SFTPUploadChunk writes a base64-encoded chunk to the open remote file.
// Each call is independent; JS awaits the return value to get natural
// backpressure without a chunk queue.
func (a *App) SFTPUploadChunk(sessionID string, b64 string) string {
	v, ok := a.sftpUploads.Load(sessionID)
	if !ok {
		return sshErrJSON("unknown session")
	}
	state := v.(*sftpUploadState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return sshErrJSON("session closed")
	}
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return sshErrJSON(err.Error())
	}
	if _, err := state.handle.Write(data); err != nil {
		return sshErrJSON(err.Error())
	}
	return `{"ok":true}`
}

// SFTPUploadFinish closes the remote file and releases the session.
func (a *App) SFTPUploadFinish(sessionID string) string {
	v, ok := a.sftpUploads.LoadAndDelete(sessionID)
	if !ok {
		return sshErrJSON("unknown session")
	}
	state := v.(*sftpUploadState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return `{"ok":true}`
	}
	state.closed = true
	if err := state.handle.Close(); err != nil {
		return sshErrJSON(err.Error())
	}
	return `{"ok":true}`
}

// SFTPUploadCancel aborts an in-progress upload. The partially-written file
// is left on the remote host (matching sftp semantics — no automatic unlink).
func (a *App) SFTPUploadCancel(sessionID string) string {
	v, ok := a.sftpUploads.LoadAndDelete(sessionID)
	if !ok {
		return `{"ok":true}`
	}
	state := v.(*sftpUploadState)
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.closed {
		state.closed = true
		state.handle.Close()
	}
	return `{"ok":true}`
}
