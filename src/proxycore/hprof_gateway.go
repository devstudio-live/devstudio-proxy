package proxycore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ── HPROF Job Types ─────────────────────────────────────────────────────────

// HprofJobStatus represents the lifecycle state of a parse job.
type HprofJobStatus string

const (
	HprofJobPending  HprofJobStatus = "pending"
	HprofJobRunning  HprofJobStatus = "running"
	HprofJobDone     HprofJobStatus = "done"
	HprofJobError    HprofJobStatus = "error"
	HprofJobCanceled HprofJobStatus = "canceled"
)

// HprofJob holds state for a single .hprof parse job.
type HprofJob struct {
	ID        string         `json:"id"`
	Path      string         `json:"path"`
	Status    HprofJobStatus `json:"status"`
	Error     string         `json:"error,omitempty"`
	CreatedAt time.Time      `json:"createdAt"`
	DoneAt    time.Time      `json:"doneAt,omitempty"`

	// Result populated after successful parse
	Result *HprofResult `json:"-"`

	// Phase 5: diff result (only populated for comparison jobs)
	diffResult *HprofDiffResult

	// Cancel channel
	cancel chan struct{}
	once   sync.Once

	// SSE subscribers
	subs   []chan []byte
	subsMu sync.Mutex
}

func (j *HprofJob) Cancel() {
	j.once.Do(func() { close(j.cancel) })
}

func (j *HprofJob) addSub(ch chan []byte) {
	j.subsMu.Lock()
	j.subs = append(j.subs, ch)
	j.subsMu.Unlock()
}

func (j *HprofJob) removeSub(ch chan []byte) {
	j.subsMu.Lock()
	for i, c := range j.subs {
		if c == ch {
			j.subs = append(j.subs[:i], j.subs[i+1:]...)
			break
		}
	}
	j.subsMu.Unlock()
}

func (j *HprofJob) broadcast(frame []byte) {
	j.subsMu.Lock()
	for _, ch := range j.subs {
		select {
		case ch <- frame:
		default: // drop if subscriber is slow
		}
	}
	j.subsMu.Unlock()
}

func sseFrame(event, data string) []byte {
	return []byte(fmt.Sprintf("event: %s\ndata: %s\n\n", event, data))
}

// ── Gateway Handler ─────────────────────────────────────────────────────────

func (s *Server) handleHprofGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)

	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-DevStudio-Gateway-Route, X-DevStudio-Gateway-Protocol")
		w.WriteHeader(http.StatusNoContent)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")

	switch {
	case path == "hprof/analyze" && r.Method == http.MethodPost:
		s.hprofAnalyze(w, r)
	case path == "hprof/query" && r.Method == http.MethodPost:
		s.hprofQuery(w, r)
	case path == "hprof/compare" && r.Method == http.MethodPost:
		s.hprofCompare(w, r)
	case path == "hprof/report" && r.Method == http.MethodPost:
		s.hprofReport(w, r)
	case strings.HasPrefix(path, "hprof/result/"):
		s.hprofResult(w, r, path)
	case strings.HasPrefix(path, "hprof/status/"):
		s.hprofStatus(w, r, path)
	case strings.HasPrefix(path, "hprof/cancel/"):
		s.hprofCancel(w, r, path)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "unknown hprof endpoint"})
	}
}

// ── POST /hprof/analyze ─────────────────────────────────────────────────────

type hprofAnalyzeReq struct {
	Path string `json:"path"`
}

func (s *Server) hprofAnalyze(w http.ResponseWriter, r *http.Request) {
	var req hprofAnalyzeReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	req.Path = strings.TrimSpace(req.Path)
	if req.Path == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "path is required"})
		return
	}

	// Validate file exists and is readable
	info, err := os.Stat(req.Path)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "file not found: " + err.Error()})
		return
	}
	if info.IsDir() {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "path is a directory, not a file"})
		return
	}

	// Phase 3: Session cache lookup — return cached result instantly
	if sess, ok := s.hprofSessions.Lookup(req.Path); ok && sess.Result != nil {
		s.hprofAnalyzeCached(w, r, sess)
		return
	}

	// Create job
	job := &HprofJob{
		ID:        uuid.New().String(),
		Path:      req.Path,
		Status:    HprofJobPending,
		CreatedAt: time.Now(),
		cancel:    make(chan struct{}),
	}
	s.hprofJobs.Store(job.ID, job)

	// Set up SSE streaming
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "streaming not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	// Subscribe to job events
	ch := make(chan []byte, 64)
	job.addSub(ch)
	defer job.removeSub(ch)

	// Send initial event with job ID
	initData, _ := json.Marshal(map[string]string{"jobId": job.ID})
	_, _ = w.Write(sseFrame("init", string(initData)))
	flusher.Flush()

	// Start parse in background
	go s.runHprofParse(job)

	// Stream events until done/error/cancel or client disconnect
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-job.cancel:
			errData, _ := json.Marshal(map[string]string{"error": "canceled"})
			_, _ = w.Write(sseFrame("error", string(errData)))
			flusher.Flush()
			return
		case frame, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(frame)
			flusher.Flush()
			// Check if this was a done or error event — terminate SSE
			if strings.HasPrefix(string(frame), "event: done") || strings.HasPrefix(string(frame), "event: error") {
				return
			}
		}
	}
}

// hprofAnalyzeCached handles a cache-hit: creates a job entry and sends SSE immediately.
func (s *Server) hprofAnalyzeCached(w http.ResponseWriter, r *http.Request, sess *hprofSession) {
	job := &HprofJob{
		ID:        uuid.New().String(),
		Path:      sess.Key.Path,
		Status:    HprofJobDone,
		CreatedAt: time.Now(),
		DoneAt:    time.Now(),
		Result:    sess.Result,
		cancel:    make(chan struct{}),
	}
	s.hprofJobs.Store(job.ID, job)

	flusher, ok := w.(http.Flusher)
	if !ok {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "streaming not supported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	res := sess.Result

	// Send init
	initData, _ := json.Marshal(map[string]string{"jobId": job.ID})
	_, _ = w.Write(sseFrame("init", string(initData)))
	flusher.Flush()

	// Send done with cached flag
	doneData, _ := json.Marshal(map[string]interface{}{
		"jobId":             job.ID,
		"cached":            true,
		"totalObjects":      res.TotalObjects,
		"totalShallowBytes": res.TotalShallowBytes,
		"classCount":        res.ClassCount,
		"gcRootCount":       res.GCRootCount,
		"timestamp":         res.Timestamp,
		"version":           res.Version,
		"idSize":            res.IDSize,
		"fileSize":          res.FileSize,
		"mode":              "proxy",
		"typeSummary":       res.TopClasses(50),
	})
	_, _ = w.Write(sseFrame("done", string(doneData)))
	flusher.Flush()
}

// runHprofParse orchestrates the two-pass parse and broadcasts SSE events.
func (s *Server) runHprofParse(job *HprofJob) {
	job.Status = HprofJobRunning

	progressCb := func(phase string, pct float64, detail string) {
		data, _ := json.Marshal(map[string]interface{}{
			"phase":  phase,
			"pct":    pct,
			"detail": detail,
		})
		job.broadcast(sseFrame("progress", string(data)))
	}

	result, err := ParseHprof(job.Path, job.cancel, progressCb)
	if err != nil {
		job.Status = HprofJobError
		job.Error = err.Error()
		errData, _ := json.Marshal(map[string]string{"error": err.Error()})
		job.broadcast(sseFrame("error", string(errData)))
		return
	}

	// Phase 3: Run deep analysis and cache
	result.StringDedup = AnalyzeStringDedup(result)
	result.Collections = AnalyzeCollections(result)

	job.Result = result
	job.Status = HprofJobDone
	job.DoneAt = time.Now()

	// Phase 3: Store in session cache
	s.hprofSessions.Store(job.Path, result, job.ID)

	// Send pass1-done event
	pass1Data, _ := json.Marshal(map[string]interface{}{
		"fileSize":     result.FileSize,
		"version":      result.Version,
		"idSize":       result.IDSize,
		"timestamp":    result.Timestamp,
		"totalObjects": result.TotalObjects,
		"classCount":   result.ClassCount,
		"gcRootCount":  result.GCRootCount,
	})
	job.broadcast(sseFrame("pass1-done", string(pass1Data)))

	// Send analysis-done event with summary
	doneData, _ := json.Marshal(map[string]interface{}{
		"jobId":             job.ID,
		"totalObjects":      result.TotalObjects,
		"totalShallowBytes": result.TotalShallowBytes,
		"classCount":        result.ClassCount,
		"gcRootCount":       result.GCRootCount,
		"timestamp":         result.Timestamp,
		"version":           result.Version,
		"idSize":            result.IDSize,
		"fileSize":          result.FileSize,
		"mode":              "proxy",
		"typeSummary":       result.TopClasses(50),
	})
	job.broadcast(sseFrame("done", string(doneData)))
}

// ── Result Sub-Endpoints ────────────────────────────────────────────────────

func (s *Server) hprofResult(w http.ResponseWriter, r *http.Request, path string) {
	w.Header().Set("Content-Type", "application/json")

	// path = "hprof/result/{id}/histogram" etc.
	parts := strings.SplitN(path, "/", 4) // ["hprof", "result", "{id}", "histogram"]
	if len(parts) < 4 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing result endpoint"})
		return
	}
	jobID := parts[2]
	endpoint := parts[3]

	raw, ok := s.hprofJobs.Load(jobID)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	job := raw.(*HprofJob)

	// Phase 5: delta endpoint works on diff jobs (no HprofResult, only diffResult)
	if endpoint == "delta" && job.Status == HprofJobDone && job.diffResult != nil {
		s.hprofDelta(w, r, job)
		return
	}

	if job.Status != HprofJobDone || job.Result == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not done", "status": string(job.Status)})
		return
	}

	switch endpoint {
	case "histogram":
		s.hprofHistogram(w, r, job.Result)
	case "gcroots":
		s.hprofGCRoots(w, r, job.Result)
	case "dominator":
		s.hprofDominator(w, r, job.Result)
	case "insights":
		s.hprofInsights(w, r, job.Result)
	case "instances":
		s.hprofInstances(w, r, job.Result)
	case "retainers":
		s.hprofRetainers(w, r, job.Result)
	case "fields":
		s.hprofFields(w, r, job.Result)
	case "hexdump":
		s.hprofHexDump(w, r, job.Result)
	case "strings":
		s.hprofStrings(w, r, job.Result)
	case "collections":
		s.hprofCollections(w, r, job.Result)
	case "flamegraph":
		s.hprofFlameGraph(w, r, job.Result)
	case "classloaders":
		s.hprofClassLoaders(w, r, job.Result)
	case "threads":
		s.hprofThreads(w, r, job.Result)
	case "topobjects":
		s.hprofTopObjects(w, r, job.Result)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "unknown endpoint: " + endpoint})
	}
}

// GET /hprof/result/{id}/histogram?offset=0&limit=200&sort=retainedSize&filter=…
func (s *Server) hprofHistogram(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	q := r.URL.Query()
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	sortCol := q.Get("sort")
	filter := strings.ToLower(q.Get("filter"))

	if limit <= 0 {
		limit = 200
	}
	if limit > 5000 {
		limit = 5000
	}

	rows := make([]HprofClassSummary, 0, len(res.ClassSummary))
	for _, cs := range res.ClassSummary {
		if filter != "" && !strings.Contains(strings.ToLower(cs.ClassName), filter) {
			continue
		}
		rows = append(rows, cs)
	}

	// Sort
	switch sortCol {
	case "instanceCount":
		sort.Slice(rows, func(i, j int) bool { return rows[i].InstanceCount > rows[j].InstanceCount })
	case "shallowSize":
		sort.Slice(rows, func(i, j int) bool { return rows[i].ShallowSize > rows[j].ShallowSize })
	case "className":
		sort.Slice(rows, func(i, j int) bool { return rows[i].ClassName < rows[j].ClassName })
	default: // retainedSize
		sort.Slice(rows, func(i, j int) bool { return rows[i].RetainedSize > rows[j].RetainedSize })
	}

	total := len(rows)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}
	page := rows[offset:end]

	json.NewEncoder(w).Encode(map[string]interface{}{
		"rows":  page,
		"total": total,
	})
}

// GET /hprof/result/{id}/gcroots
func (s *Server) hprofGCRoots(w http.ResponseWriter, _ *http.Request, res *HprofResult) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"gcRoots": res.GCRoots,
	})
}

// GET /hprof/result/{id}/dominator?level=class|object&nodeIndex=...&offset=0&limit=50
func (s *Server) hprofDominator(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	q := r.URL.Query()
	level := q.Get("level")

	if level == "object" {
		s.hprofObjectDominator(w, r, res)
		return
	}

	// Class-level dominator tree
	nodeIndex := q.Get("nodeIndex")
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}

	// nodeIndex=-1 or empty → return top-level classes as children
	// nodeIndex=<classId> → class-level tree is flat, no children below classes
	if nodeIndex != "" && nodeIndex != "-1" {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"nodes": []interface{}{},
			"total": 0,
		})
		return
	}

	rows := make([]map[string]interface{}, 0, len(res.ClassSummary))
	for _, cs := range res.ClassSummary {
		rows = append(rows, map[string]interface{}{
			"classId":      cs.ClassID,
			"className":    cs.ClassName,
			"count":        cs.InstanceCount,
			"shallowSize":  cs.ShallowSize,
			"retainedSize": cs.RetainedSize,
			"hasChildren":  false,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		ri, _ := rows[i]["retainedSize"].(int64)
		rj, _ := rows[j]["retainedSize"].(int64)
		return ri > rj
	})

	total := len(rows)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": rows[offset:end],
		"total": total,
	})
}

// Object-level dominator tree
func (s *Server) hprofObjectDominator(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	og := res.ObjectGraph
	if og == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"nodes": []interface{}{},
			"total": 0,
		})
		return
	}

	q := r.URL.Query()
	nodeIndex, _ := strconv.Atoi(q.Get("nodeIndex"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 50
	}
	if nodeIndex < 0 || nodeIndex >= og.NodeCount {
		nodeIndex = 0
	}

	children := og.DomChildren[nodeIndex]
	total := len(children)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	nodes := make([]map[string]interface{}, 0, end-offset)
	for _, childIdx := range children[offset:end] {
		className := og.ClassNames[og.ClassIDs[childIdx]]
		if className == "" {
			className = formatID(og.ClassIDs[childIdx])
		}
		nodes = append(nodes, map[string]interface{}{
			"index":        childIdx,
			"objectId":     formatID(og.ObjectIDs[childIdx]),
			"className":    className,
			"shallowSize":  og.ShallowSizes[childIdx],
			"retainedSize": og.RetainedSizes[childIdx],
			"hasChildren":  og.HasChildren[childIdx],
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"nodes": nodes,
		"total": total,
	})
}

// GET /hprof/result/{id}/insights
func (s *Server) hprofInsights(w http.ResponseWriter, _ *http.Request, res *HprofResult) {
	json.NewEncoder(w).Encode(map[string]interface{}{
		"mode":  "proxy",
		"cards": res.Insights,
	})
}

// GET /hprof/result/{id}/instances?classId=0x1234&offset=0&limit=100
func (s *Server) hprofInstances(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	og := res.ObjectGraph
	if og == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"instances": []interface{}{},
			"total":     0,
		})
		return
	}

	q := r.URL.Query()
	classIDStr := q.Get("classId")
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	classID := parseHexID(classIDStr)

	nodeIndices := og.ClassToNodes[classID]
	// Sort by retained size descending
	sorted := make([]int32, len(nodeIndices))
	copy(sorted, nodeIndices)
	sort.Slice(sorted, func(i, j int) bool {
		return og.RetainedSizes[sorted[i]] > og.RetainedSizes[sorted[j]]
	})

	total := len(sorted)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	instances := make([]map[string]interface{}, 0, end-offset)
	for _, idx := range sorted[offset:end] {
		instances = append(instances, map[string]interface{}{
			"objectId":     formatID(og.ObjectIDs[idx]),
			"shallowSize":  og.ShallowSizes[idx],
			"retainedSize": og.RetainedSizes[idx],
		})
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"instances": instances,
		"total":     total,
	})
}

// GET /hprof/result/{id}/retainers?objectId=0x1234abcd
func (s *Server) hprofRetainers(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	og := res.ObjectGraph
	if og == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"paths": []interface{}{},
		})
		return
	}

	objectIDStr := r.URL.Query().Get("objectId")
	objectID := parseHexID(objectIDStr)

	idx, ok := og.IDToIndex[objectID]
	if !ok {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"paths": []interface{}{},
		})
		return
	}

	paths := FindRetainerPaths(og, idx, 5, 20)
	if paths == nil {
		paths = [][]RetainerNode{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"objectId": objectIDStr,
		"paths":    paths,
	})
}

// GET /hprof/result/{id}/fields?objectId=0x1234abcd
func (s *Server) hprofFields(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	og := res.ObjectGraph
	if og == nil || res.mmapData == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "object graph not available"})
		return
	}

	objectIDStr := r.URL.Query().Get("objectId")
	objectID := parseHexID(objectIDStr)

	idx, ok := og.IDToIndex[objectID]
	if !ok {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"objectId":  objectIDStr,
			"className": "?",
			"fields":    []interface{}{},
			"error":     "object not found",
		})
		return
	}

	className := og.ClassNames[og.ClassIDs[idx]]
	if className == "" {
		className = formatID(og.ClassIDs[idx])
	}

	fields := og.DecodeFields(res.mmapData, idx)
	if fields == nil {
		fields = []DecodedField{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"objectId":  objectIDStr,
		"className": className,
		"fields":    fields,
	})
}

// GET /hprof/result/{id}/hexdump?objectId=0x1234abcd
func (s *Server) hprofHexDump(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	og := res.ObjectGraph
	if og == nil || res.mmapData == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "object graph not available"})
		return
	}

	objectIDStr := r.URL.Query().Get("objectId")
	objectID := parseHexID(objectIDStr)

	idx, ok := og.IDToIndex[objectID]
	if !ok {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"bytes":  []byte{},
			"fields": []interface{}{},
		})
		return
	}

	fileOff := og.FileOffsets[idx]
	recLen := int64(og.RecordLens[idx])
	readLen := recLen
	if readLen > 8192 {
		readLen = 8192
	}

	end := fileOff + readLen
	if end > int64(len(res.mmapData)) {
		end = int64(len(res.mmapData))
	}
	rawBytes := res.mmapData[fileOff:end]

	// Convert to int array (matching worker format)
	byteArr := make([]int, len(rawBytes))
	for i, b := range rawBytes {
		byteArr[i] = int(b)
	}

	// Decode fields for annotation
	fields := og.DecodeFields(res.mmapData, idx)
	if fields == nil {
		fields = []DecodedField{}
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"bytes":  byteArr,
		"fields": fields,
	})
}

// ── Phase 3: String Dedup + Collections Endpoints ──────────────────────────

// GET /hprof/result/{id}/strings?minDuplicates=2&offset=0&limit=100&sort=waste
func (s *Server) hprofStrings(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	sd := res.StringDedup
	if sd == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries":     []interface{}{},
			"totalWaste":  0,
			"uniqueCount": 0,
			"total":       0,
		})
		return
	}

	q := r.URL.Query()
	minDup, _ := strconv.Atoi(q.Get("minDuplicates"))
	if minDup < 2 {
		minDup = 2
	}
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}
	sortCol := q.Get("sort")

	// Filter by minDuplicates
	filtered := make([]StringDedupEntry, 0, len(sd.Entries))
	for _, e := range sd.Entries {
		if e.Count >= int64(minDup) {
			filtered = append(filtered, e)
		}
	}

	// Sort
	switch sortCol {
	case "count":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].Count > filtered[j].Count })
	case "value":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].Value < filtered[j].Value })
	default: // "waste"
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].WastedBytes > filtered[j].WastedBytes })
	}

	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"entries":     filtered[offset:end],
		"totalWaste":  sd.TotalWaste,
		"uniqueCount": sd.UniqueCount,
		"total":       total,
	})
}

// GET /hprof/result/{id}/collections?filter=empty|oversized|all&offset=0&limit=100
func (s *Server) hprofCollections(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	ca := res.Collections
	if ca == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"entries": []interface{}{},
			"total":   0,
		})
		return
	}

	q := r.URL.Query()
	filter := strings.ToLower(q.Get("filter"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	filtered := make([]CollectionEntry, 0, len(ca.Entries))
	for _, e := range ca.Entries {
		switch filter {
		case "empty":
			if e.Category != "empty" {
				continue
			}
		case "oversized":
			if e.Category != "oversized" {
				continue
			}
		}
		filtered = append(filtered, e)
	}

	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"entries": filtered[offset:end],
		"total":   total,
	})
}

// ── Phase 4: OQL Query + Flame Graph Endpoints ──────────────────────────────

// POST /hprof/query   body: {"jobId": "...", "oql": "SELECT ...", "offset": 0, "limit": 100}
func (s *Server) hprofQuery(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		JobID  string `json:"jobId"`
		OQL    string `json:"oql"`
		Offset int    `json:"offset"`
		Limit  int    `json:"limit"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if req.JobID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "jobId is required"})
		return
	}
	if req.OQL == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "oql is required"})
		return
	}

	raw, ok := s.hprofJobs.Load(req.JobID)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	job := raw.(*HprofJob)
	if job.Status != HprofJobDone || job.Result == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not done", "status": string(job.Status)})
		return
	}

	query, err := ParseOQL(req.OQL)
	if err != nil {
		json.NewEncoder(w).Encode(&OQLResult{Error: err.Error()})
		return
	}

	// Override offset/limit from request if provided
	if req.Offset > 0 {
		query.Offset = req.Offset
	}
	if req.Limit > 0 {
		query.Limit = req.Limit
	}

	result := ExecuteOQL(query, job.Result)
	json.NewEncoder(w).Encode(result)
}

// GET /hprof/result/{id}/flamegraph
func (s *Server) hprofFlameGraph(w http.ResponseWriter, _ *http.Request, res *HprofResult) {
	fg := BuildFlameGraph(res)
	if fg == nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"root":           nil,
			"hasStackTraces": false,
			"totalAllocated": 0,
		})
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"root":           fg,
		"hasStackTraces": true,
		"totalAllocated": fg.Value,
	})
}

// parseHexID parses "0x1234abcd" or "1234abcd" to uint64.
func parseHexID(s string) uint64 {
	s = strings.TrimPrefix(s, "0x")
	v, _ := strconv.ParseUint(s, 16, 64)
	return v
}

// ── Status / Cancel ─────────────────────────────────────────────────────────

func (s *Server) hprofStatus(w http.ResponseWriter, _ *http.Request, path string) {
	w.Header().Set("Content-Type", "application/json")

	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing job ID"})
		return
	}
	jobID := parts[2]

	raw, ok := s.hprofJobs.Load(jobID)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	job := raw.(*HprofJob)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"id":     job.ID,
		"status": job.Status,
		"error":  job.Error,
	})
}

func (s *Server) hprofCancel(w http.ResponseWriter, r *http.Request, path string) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "POST required"})
		return
	}

	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing job ID"})
		return
	}
	jobID := parts[2]

	raw, ok := s.hprofJobs.Load(jobID)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	job := raw.(*HprofJob)
	job.Cancel()
	job.Status = HprofJobCanceled

	json.NewEncoder(w).Encode(map[string]string{"status": "canceled"})
}

// ── Phase 5: Heap Diff / Comparison Endpoints ──────────────────────────────

// POST /hprof/compare   body: {"jobA": "uuid", "jobB": "uuid"} or {"pathA": "...", "pathB": "..."}
func (s *Server) hprofCompare(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req struct {
		JobA  string `json:"jobA"`
		JobB  string `json:"jobB"`
		PathA string `json:"pathA"`
		PathB string `json:"pathB"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	// Resolve results — prefer jobA/jobB, fall back to pathA/pathB via session cache
	resolveResult := func(jobID, path string) (*HprofResult, string) {
		if jobID != "" {
			if raw, ok := s.hprofJobs.Load(jobID); ok {
				job := raw.(*HprofJob)
				if job.Status == HprofJobDone && job.Result != nil {
					return job.Result, ""
				}
				return nil, "job not done: " + string(job.Status)
			}
			return nil, "job not found: " + jobID
		}
		if path != "" {
			if sess, ok := s.hprofSessions.Lookup(path); ok && sess.Result != nil {
				return sess.Result, ""
			}
			return nil, "no cached session for path: " + path
		}
		return nil, "jobId or path required"
	}

	resultA, errA := resolveResult(req.JobA, req.PathA)
	if errA != "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "dump A: " + errA})
		return
	}
	resultB, errB := resolveResult(req.JobB, req.PathB)
	if errB != "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "dump B: " + errB})
		return
	}

	diff := CompareHprofResults(resultA, resultB)

	// Store the diff result as a synthetic job so the /delta endpoint can serve paginated queries
	diffJob := &HprofJob{
		ID:        uuid.New().String(),
		Path:      "(diff)",
		Status:    HprofJobDone,
		CreatedAt: time.Now(),
		DoneAt:    time.Now(),
		cancel:    make(chan struct{}),
	}
	diffJob.diffResult = diff
	s.hprofJobs.Store(diffJob.ID, diffJob)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"diffId":          diffJob.ID,
		"totalCountA":     diff.TotalCountA,
		"totalCountB":     diff.TotalCountB,
		"totalRetainedA":  diff.TotalRetainedA,
		"totalRetainedB":  diff.TotalRetainedB,
		"newClassCount":   diff.NewClassCount,
		"freedClassCount": diff.FreedClassCount,
		"deltaCount":      len(diff.Deltas),
	})
}

// GET /hprof/result/{diffId}/delta?sort=retainedDelta&offset=0&limit=100&filter=grew|shrunk|new|freed
func (s *Server) hprofDelta(w http.ResponseWriter, r *http.Request, job *HprofJob) {
	diff := job.diffResult
	if diff == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "no diff result on this job"})
		return
	}

	q := r.URL.Query()
	sortCol := q.Get("sort")
	filter := strings.ToLower(q.Get("filter"))
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	// Filter
	filtered := make([]HprofClassDelta, 0, len(diff.Deltas))
	for _, d := range diff.Deltas {
		if filter != "" && d.Category != filter {
			continue
		}
		filtered = append(filtered, d)
	}

	// Sort
	switch sortCol {
	case "countDelta":
		sort.Slice(filtered, func(i, j int) bool { return abs64(filtered[i].CountDelta) > abs64(filtered[j].CountDelta) })
	case "retainedA":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].RetainedA > filtered[j].RetainedA })
	case "retainedB":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].RetainedB > filtered[j].RetainedB })
	case "className":
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].ClassName < filtered[j].ClassName })
	default: // "retainedDelta"
		sort.Slice(filtered, func(i, j int) bool { return abs64(filtered[i].RetainedDelta) > abs64(filtered[j].RetainedDelta) })
	}

	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"rows":  filtered[offset:end],
		"total": total,
	})
}

// ── Phase 6: Class Loaders ──────────────────────────────────────────────────

// GET /hprof/result/{id}/classloaders
func (s *Server) hprofClassLoaders(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	// Lazy compute on first access
	if res.ClassLoaders == nil {
		res.ClassLoaders = AnalyzeClassLoaders(res)
	}
	json.NewEncoder(w).Encode(res.ClassLoaders)
}

// ── Phase 6: Threads ────────────────────────────────────────────────────────

// GET /hprof/result/{id}/threads?sort=retainedSize
func (s *Server) hprofThreads(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	// Lazy compute on first access
	if res.ThreadRetained == nil {
		res.ThreadRetained = AnalyzeThreads(res)
	}

	q := r.URL.Query()
	sortCol := q.Get("sort")

	threads := make([]ThreadEntry, len(res.ThreadRetained.Threads))
	copy(threads, res.ThreadRetained.Threads)

	switch sortCol {
	case "threadName":
		sort.Slice(threads, func(i, j int) bool { return threads[i].ThreadName < threads[j].ThreadName })
	case "retainedObjects":
		sort.Slice(threads, func(i, j int) bool { return threads[i].RetainedObjs > threads[j].RetainedObjs })
	default: // "retainedSize"
		sort.Slice(threads, func(i, j int) bool { return threads[i].RetainedSize > threads[j].RetainedSize })
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"threads":    threads,
		"totalCount": res.ThreadRetained.TotalCount,
	})
}

// GET /hprof/result/{id}/topobjects?offset=0&limit=100
func (s *Server) hprofTopObjects(w http.ResponseWriter, r *http.Request, res *HprofResult) {
	q := r.URL.Query()
	offset, _ := strconv.Atoi(q.Get("offset"))
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	total := len(res.TopObjects)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"objects": res.TopObjects[offset:end],
		"total":   total,
	})
}

// ── Phase 6: Report Generation ──────────────────────────────────────────────

// POST /hprof/report  body: {"jobId": "...", "format": "html", "includeAI": true}
func (s *Server) hprofReport(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req HprofReportReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}

	if req.JobID == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "jobId is required"})
		return
	}

	raw, ok := s.hprofJobs.Load(req.JobID)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not found"})
		return
	}
	job := raw.(*HprofJob)

	if job.Status != HprofJobDone || job.Result == nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "job not done", "status": string(job.Status)})
		return
	}

	// Ensure Phase 6 analyses are computed before report
	if job.Result.ClassLoaders == nil {
		job.Result.ClassLoaders = AnalyzeClassLoaders(job.Result)
	}
	if job.Result.ThreadRetained == nil {
		job.Result.ThreadRetained = AnalyzeThreads(job.Result)
	}

	report, err := GenerateHprofReport(job.Result, req.IncludeAI)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": "report generation failed: " + err.Error()})
		return
	}

	json.NewEncoder(w).Encode(report)
}

// ── Job Reaper (30-min TTL) ─────────────────────────────────────────────────

const hprofJobTTL = 30 * time.Minute
const hprofReapInterval = 5 * time.Minute

func (s *Server) startHprofJobReaper() {
	ticker := time.NewTicker(hprofReapInterval)
	for range ticker.C {
		s.reapHprofJobs()
	}
}

func (s *Server) reapHprofJobs() {
	now := time.Now()
	s.hprofJobs.Range(func(k, v any) bool {
		job := v.(*HprofJob)
		age := now.Sub(job.CreatedAt)
		if age > hprofJobTTL {
			job.Cancel()
			if job.Result != nil {
				job.Result.Close()
			}
			s.hprofJobs.Delete(k)
		}
		return true
	})
}
