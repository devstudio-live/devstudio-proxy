package proxycore

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
)

// ── HPROF Binary Format Constants ───────────────────────────────────────────

const (
	tagString        = 0x01
	tagLoadClass     = 0x02
	tagStackFrame    = 0x04
	tagStackTrace    = 0x05
	tagHeapDump      = 0x0C
	tagHeapDumpSeg   = 0x1C
	tagHeapDumpEnd   = 0x2C

	// Heap dump sub-record tags
	heapGCRootUnknown     = 0xFF
	heapGCRootJNIGlobal   = 0x01
	heapGCRootJNILocal    = 0x02
	heapGCRootJavaFrame   = 0x03
	heapGCRootNativeStack = 0x04
	heapGCRootStickyClass = 0x05
	heapGCRootThreadBlock = 0x06
	heapGCRootMonitor     = 0x07
	heapGCRootThreadObj   = 0x08
	heapClassDump         = 0x20
	heapInstanceDump      = 0x21
	heapObjectArrayDump   = 0x22
	heapPrimArrayDump     = 0x23
)

// Primitive type sizes (used in CLASS_DUMP field descriptors and PRIMITIVE_ARRAY_DUMP)
var primSizes = map[byte]int{
	2:  0, // object reference — size is idSize
	4:  1, // boolean
	5:  2, // char
	6:  4, // float
	7:  8, // double
	8:  1, // byte
	9:  2, // short
	10: 4, // int
	11: 8, // long
}

// ── Result Types ────────────────────────────────────────────────────────────

// HprofResult holds the parsed analysis results.
type HprofResult struct {
	FileSize          int64  `json:"fileSize"`
	Version           string `json:"version"`
	IDSize            int    `json:"idSize"`
	Timestamp         int64  `json:"timestamp"`
	TotalObjects      int64  `json:"totalObjects"`
	TotalShallowBytes int64  `json:"totalShallowBytes"`
	ClassCount        int    `json:"classCount"`
	GCRootCount       int    `json:"gcRootCount"`

	ClassSummary  []HprofClassSummary `json:"classSummary"`
	GCRoots       []HprofGCRootGroup  `json:"gcRoots"`
	DominatorTree []HprofDomNode      `json:"dominatorTree"`
	Insights      []HprofInsight      `json:"insights"`

	// Phase 2: full object graph + per-object dominator tree (nil if not computed)
	ObjectGraph *ObjectGraph

	// Phase 3: deep analysis (nil until computed)
	StringDedup *StringDedupResult  `json:"-"`
	Collections *CollectionAnalysis `json:"-"`

	// Phase 4: stack traces + allocation sites (for OQL and flame graph)
	Strings                map[uint64]string            `json:"-"` // stringID → UTF-8 (from pass1)
	StackFrames            map[uint64]HprofStackFrame   `json:"-"`
	StackTraces            map[uint32]*HprofStackTrace  `json:"-"`
	AllocSites             []AllocSite                  `json:"-"`
	FrameClassSerialToObjID map[uint32]uint64            `json:"-"` // class serial → classObjectID

	// Phase 6: class loader + thread analysis (nil until computed)
	ClassLoaderMap map[uint64]uint64           `json:"-"` // classObjectID → classLoaderObjectID
	ThreadObjects  map[uint64]ThreadObjectMeta `json:"-"` // objectID → thread metadata
	ClassLoaders   *ClassLoaderAnalysis        `json:"-"`
	ThreadRetained *ThreadRetainedAnalysis     `json:"-"`

	// Top objects by shallow size (for top objects endpoint)
	TopObjects []TopObjectEntry `json:"-"`

	// Internal — for cleanup
	mmapData []byte
}

// Close releases mmapped memory.
func (r *HprofResult) Close() {
	if r.mmapData != nil {
		_ = munmapFile(r.mmapData)
		r.mmapData = nil
	}
}

// TopClasses returns the top N classes by retained size for the SSE summary.
func (r *HprofResult) TopClasses(n int) []HprofClassSummary {
	sorted := make([]HprofClassSummary, len(r.ClassSummary))
	copy(sorted, r.ClassSummary)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].RetainedSize > sorted[j].RetainedSize })
	if n > len(sorted) {
		n = len(sorted)
	}
	return sorted[:n]
}

// TopObjectEntry is a single large object for the top objects endpoint.
type TopObjectEntry struct {
	ObjectID    string `json:"objectId"`
	ClassName   string `json:"className"`
	ClassID     string `json:"classId"`
	ShallowSize int64  `json:"shallowSize"`
	Kind        string `json:"kind"`
	ElemType    byte   `json:"elemType"`
}

// HprofClassSummary is a per-class aggregation (histogram row).
type HprofClassSummary struct {
	ClassID       string `json:"classId"`
	ClassName     string `json:"className"`
	InstanceCount int64  `json:"count"`
	ShallowSize   int64  `json:"shallowSize"`
	RetainedSize  int64  `json:"retainedSize"`
}

// HprofGCRootGroup groups GC roots by type.
type HprofGCRootGroup struct {
	Type      string              `json:"type"`
	Count     int                 `json:"count"`
	ObjectIDs []HprofGCRootEntry  `json:"objectIds"`
}

// HprofGCRootEntry is a single GC root reference.
type HprofGCRootEntry struct {
	ObjectID  string `json:"objectId"`
	ClassName string `json:"className"`
}

// HprofDomNode is a class-level dominator tree node.
type HprofDomNode struct {
	ClassName    string `json:"className"`
	ClassID      string `json:"classId"`
	RetainedSize int64  `json:"retainedSize"`
	ShallowSize  int64  `json:"shallowSize"`
	ObjectCount  int64  `json:"objectCount"`
}

// HprofInsight is a triage insight card.
type HprofInsight struct {
	ID       string              `json:"id"`
	Title    string              `json:"title"`
	Severity string              `json:"severity"`
	Summary  string              `json:"summary"`
	Metrics  []HprofMetric       `json:"metrics"`
	Rows     []map[string]interface{} `json:"rows"`
	Actions  []HprofAction       `json:"actions"`
}

// HprofMetric is a key-value metric in an insight card.
type HprofMetric struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

// HprofAction is a navigational action in an insight card.
type HprofAction struct {
	Label      string `json:"label"`
	TargetView string `json:"targetView"`
	ClassID    string `json:"classId,omitempty"`
	ClassName  string `json:"className,omitempty"`
	RootType   string `json:"rootType,omitempty"`
}

// ── Parser State ────────────────────────────────────────────────────────────

type hprofParser struct {
	data     []byte
	fileSize int64
	idSize   int

	// Pass 1 results
	strings    map[uint64]string  // stringID → UTF-8 value
	classes    map[uint64]string  // classObjectID → className
	classSerial map[uint32]uint64 // serial → classObjectID
	segOffsets []int64            // offsets of HEAP_DUMP / HEAP_DUMP_SEGMENT records

	// Pass 1 stack trace data (Phase 4)
	stackFrames  map[uint64]HprofStackFrame  // frameID → frame
	stackTraces  map[uint32]*HprofStackTrace // serial → trace

	// Pass 2 results
	classDumps    map[uint64]*classDump // classObjectID → dump
	classSummary  map[uint64]*classStats
	gcRoots       map[string]int                   // rootType → count
	gcRootEntries map[string][]HprofGCRootEntry    // rootType → entries (max 20)
	topObjects    []topObject

	totalObjects      int64
	totalShallowBytes int64

	// Phase 4: allocation site tracking (populated during pass 2)
	objectStackSerials map[uint64]uint32 // objectID → stackTraceSerial

	// Phase 6: class loader + thread tracking
	classLoaderMap map[uint64]uint64           // classObjectID → classLoaderObjectID
	threadObjects  map[uint64]ThreadObjectMeta // objectID → thread meta

	// Phase 2: Object graph collection
	objectIDs        []uint64
	objectClassIDs   []uint64
	objectShallows   []int64
	objectFileOffs   []int64
	objectRecLens    []int32
	objectKinds      []byte
	objectElemTypes  []byte
	idToIndex        map[uint64]int32
	edgeSrcIdx       []int32
	edgeDstObjID     []uint64
	gcRootObjIDs     map[uint64]string // objectID → rootType
	fieldLayoutCache map[uint64][]fieldDesc
	nodeCount        int32
}

type classDump struct {
	superClassID   uint64
	instanceSize   int
	staticFields   []fieldDesc
	instanceFields []fieldDesc
}

type fieldDesc struct {
	nameID uint64
	typ    byte // 2=object, 4=boolean, 5=char, 6=float, 7=double, 8=byte, 9=short, 10=int, 11=long
}

type classStats struct {
	count        int64
	shallowTotal int64
}

type topObject struct {
	objectID    uint64
	classID     uint64
	shallowSize int64
	kind        string // "instance", "obj-array", "prim-array"
	elemType    byte   // for prim-array
}

// ── ParseHprof ──────────────────────────────────────────────────────────────

// ProgressCallback reports parsing progress to the SSE stream.
type ProgressCallback func(phase string, pct float64, detail string)

// ParseHprof opens, mmaps, and parses an .hprof file in two passes.
func ParseHprof(path string, cancel <-chan struct{}, progress ProgressCallback) (*HprofResult, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat: %w", err)
	}
	fileSize := info.Size()
	if fileSize < 32 {
		return nil, fmt.Errorf("file too small to be a valid HPROF file (%d bytes)", fileSize)
	}

	data, err := mmapFile(int(f.Fd()), int(fileSize))
	if err != nil {
		return nil, fmt.Errorf("mmap: %w", err)
	}

	p := &hprofParser{
		data:          data,
		fileSize:      fileSize,
		strings:       make(map[uint64]string),
		classes:       make(map[uint64]string),
		classSerial:   make(map[uint32]uint64),
		stackFrames:   make(map[uint64]HprofStackFrame),
		stackTraces:   make(map[uint32]*HprofStackTrace),
		classDumps:    make(map[uint64]*classDump),
		classSummary:  make(map[uint64]*classStats),
		gcRoots:       make(map[string]int),
		gcRootEntries: make(map[string][]HprofGCRootEntry),
		objectStackSerials: make(map[uint64]uint32),
		classLoaderMap:     make(map[uint64]uint64),
		threadObjects:      make(map[uint64]ThreadObjectMeta),
	}

	// Phase 2: initialize object graph collection — index 0 is virtual GC root
	p.objectIDs = []uint64{0}
	p.objectClassIDs = []uint64{0}
	p.objectShallows = []int64{0}
	p.objectFileOffs = []int64{0}
	p.objectRecLens = []int32{0}
	p.objectKinds = []byte{0}
	p.objectElemTypes = []byte{0}
	p.idToIndex = make(map[uint64]int32)
	p.gcRootObjIDs = make(map[uint64]string)
	p.fieldLayoutCache = make(map[uint64][]fieldDesc)
	p.nodeCount = 1

	// Parse header
	version, headerEnd, idSize, timestamp, err := p.parseHeader()
	if err != nil {
		_ = munmapFile(data)
		return nil, err
	}
	p.idSize = idSize

	if progress != nil {
		progress("Pass 1: Scanning index…", 0, "")
	}

	// Pass 1: collect strings, classes, segment offsets
	if err := p.pass1(headerEnd, cancel, progress); err != nil {
		_ = munmapFile(data)
		return nil, fmt.Errorf("pass1: %w", err)
	}

	// Resolve class names
	for serial, classObjID := range p.classSerial {
		_ = serial
		if nameID, ok := p.classNameID(classObjID); ok {
			if name, exists := p.strings[nameID]; exists {
				p.classes[classObjID] = javaClassName(name)
			}
		}
	}

	if progress != nil {
		progress("Pass 2: Analyzing heap…", 0, "")
	}

	// Pass 2: parse heap segments
	if err := p.pass2(cancel, progress); err != nil {
		_ = munmapFile(data)
		return nil, fmt.Errorf("pass2: %w", err)
	}

	// Phase 2: Build full object graph + dominator tree
	if progress != nil {
		progress("Building object graph…", 0, "resolving references")
	}
	og := p.buildObjectGraph(progress)

	// Build result
	result := &HprofResult{
		FileSize:          fileSize,
		Version:           version,
		IDSize:            idSize,
		Timestamp:         timestamp,
		TotalObjects:      p.totalObjects,
		TotalShallowBytes: p.totalShallowBytes,
		ClassCount:        len(p.classes),
		GCRootCount:       p.totalGCRoots(),
		mmapData:          data,
	}

	result.ClassSummary = p.buildClassSummary()
	result.GCRoots = p.buildGCRootGroups()
	result.DominatorTree = p.buildDominatorTree()
	result.TopObjects = p.buildTopObjects()
	result.Insights = p.buildInsights(result)

	// Phase 2: attach object graph and update class retained sizes
	if og != nil {
		result.ObjectGraph = og
		result.ClassSummary = p.buildClassSummaryWithRetained(og)
	}

	// Phase 4: attach stack traces + build allocation site index
	result.Strings = p.strings
	result.StackFrames = p.stackFrames
	result.StackTraces = p.stackTraces
	result.FrameClassSerialToObjID = p.classSerial
	result.AllocSites = p.buildAllocSites()

	// Phase 6: attach class loader map + thread objects for lazy analysis
	result.ClassLoaderMap = p.classLoaderMap
	result.ThreadObjects = p.threadObjects

	return result, nil
}

// ── Header Parsing ──────────────────────────────────────────────────────────

func (p *hprofParser) parseHeader() (version string, headerEnd int64, idSize int, timestamp int64, err error) {
	// Header: null-terminated version string, then u32 idSize, then u64 timestamp
	nullIdx := -1
	limit := 64
	if limit > len(p.data) {
		limit = len(p.data)
	}
	for i := 0; i < limit; i++ {
		if p.data[i] == 0 {
			nullIdx = i
			break
		}
	}
	if nullIdx < 0 {
		return "", 0, 0, 0, fmt.Errorf("invalid HPROF header: no null terminator found")
	}

	version = string(p.data[:nullIdx])
	pos := int64(nullIdx + 1)

	if pos+12 > p.fileSize {
		return "", 0, 0, 0, fmt.Errorf("header too short")
	}

	idSize = int(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	pos += 4
	if idSize != 4 && idSize != 8 {
		return "", 0, 0, 0, fmt.Errorf("unsupported id size: %d", idSize)
	}

	hi := binary.BigEndian.Uint32(p.data[pos : pos+4])
	lo := binary.BigEndian.Uint32(p.data[pos+4 : pos+8])
	timestamp = int64(hi)<<32 | int64(lo)
	pos += 8

	return version, pos, idSize, timestamp, nil
}

// ── Pass 1: Index Scan ──────────────────────────────────────────────────────

func (p *hprofParser) pass1(start int64, cancel <-chan struct{}, progress ProgressCallback) error {
	pos := start
	lastPct := -1.0
	recordCount := 0

	for pos+9 <= p.fileSize {
		// Check cancel
		if recordCount%10000 == 0 {
			select {
			case <-cancel:
				return fmt.Errorf("canceled")
			default:
			}
			if progress != nil {
				pct := float64(pos) / float64(p.fileSize) * 100
				if pct-lastPct >= 1.0 {
					progress("Pass 1: Scanning index…", math.Round(pct), "")
					lastPct = pct
				}
			}
		}
		recordCount++

		tag := p.data[pos]
		// skip 4 bytes time offset
		bodyLen := int64(binary.BigEndian.Uint32(p.data[pos+5 : pos+9]))
		bodyStart := pos + 9

		if bodyStart+bodyLen > p.fileSize {
			break // truncated record at end
		}

		switch tag {
		case tagString:
			p.parseStringRecord(bodyStart, bodyLen)
		case tagLoadClass:
			p.parseLoadClassRecord(bodyStart)
		case tagStackFrame:
			p.parseStackFrameRecord(bodyStart)
		case tagStackTrace:
			p.parseStackTraceRecord(bodyStart, bodyLen)
		case tagHeapDump, tagHeapDumpSeg:
			p.segOffsets = append(p.segOffsets, pos)
		}

		pos = bodyStart + bodyLen
	}

	return nil
}

func (p *hprofParser) parseStringRecord(bodyStart, bodyLen int64) {
	if bodyLen < int64(p.idSize) {
		return
	}
	id := p.readID(bodyStart)
	strBytes := p.data[bodyStart+int64(p.idSize) : bodyStart+bodyLen]
	p.strings[id] = string(strBytes)
}

func (p *hprofParser) parseLoadClassRecord(bodyStart int64) {
	// serial(4) + classObjID(idSize) + stackTraceSerial(4) + nameStringID(idSize)
	if bodyStart+int64(4+p.idSize+4+p.idSize) > p.fileSize {
		return
	}
	serial := binary.BigEndian.Uint32(p.data[bodyStart : bodyStart+4])
	classObjID := p.readID(bodyStart + 4)
	nameStringID := p.readID(bodyStart + int64(4+p.idSize+4))

	p.classSerial[serial] = classObjID
	if name, ok := p.strings[nameStringID]; ok {
		p.classes[classObjID] = javaClassName(name)
	}
}

// parseStackFrameRecord parses a STACK_FRAME record (tag 0x04). Phase 4.
// Layout: frameID(idSize) + methodNameID(idSize) + methodSigID(idSize) + sourceFileID(idSize) + classSerialNo(4) + lineNumber(4)
func (p *hprofParser) parseStackFrameRecord(bodyStart int64) {
	idSz := int64(p.idSize)
	need := idSz*4 + 8
	if bodyStart+need > p.fileSize {
		return
	}
	frameID := p.readID(bodyStart)
	methodNameID := p.readID(bodyStart + idSz)
	methodSigID := p.readID(bodyStart + idSz*2)
	sourceFileID := p.readID(bodyStart + idSz*3)
	classSerialNo := binary.BigEndian.Uint32(p.data[bodyStart+idSz*4 : bodyStart+idSz*4+4])
	lineNumber := int32(binary.BigEndian.Uint32(p.data[bodyStart+idSz*4+4 : bodyStart+idSz*4+8]))

	p.stackFrames[frameID] = HprofStackFrame{
		FrameID:       frameID,
		MethodNameID:  methodNameID,
		MethodSigID:   methodSigID,
		SourceFileID:  sourceFileID,
		ClassSerialNo: classSerialNo,
		LineNumber:    lineNumber,
	}
}

// parseStackTraceRecord parses a STACK_TRACE record (tag 0x05). Phase 4.
// Layout: serial(4) + threadSerial(4) + numFrames(4) + frameIDs[numFrames * idSize]
func (p *hprofParser) parseStackTraceRecord(bodyStart, bodyLen int64) {
	if bodyStart+12 > p.fileSize {
		return
	}
	serial := binary.BigEndian.Uint32(p.data[bodyStart : bodyStart+4])
	threadSerial := binary.BigEndian.Uint32(p.data[bodyStart+4 : bodyStart+8])
	numFrames := int(binary.BigEndian.Uint32(p.data[bodyStart+8 : bodyStart+12]))

	idSz := int64(p.idSize)
	framesStart := bodyStart + 12
	if framesStart+int64(numFrames)*idSz > bodyStart+bodyLen {
		numFrames = int((bodyLen - 12) / idSz)
	}

	frameIDs := make([]uint64, numFrames)
	for i := 0; i < numFrames; i++ {
		frameIDs[i] = p.readID(framesStart + int64(i)*idSz)
	}

	p.stackTraces[serial] = &HprofStackTrace{
		Serial:       serial,
		ThreadSerial: threadSerial,
		FrameIDs:     frameIDs,
	}
}

// classNameID returns the name string ID for a class from LOAD_CLASS records.
// After pass1, we have classSerial populated. We also directly store in classes map.
func (p *hprofParser) classNameID(classObjID uint64) (uint64, bool) {
	// We need a reverse lookup. Since we stored the name directly in parseLoadClassRecord,
	// this method is used as a fallback for classes loaded after their strings.
	// We'll do a second pass over LOAD_CLASS data in pass1 already, so this is rarely needed.
	return 0, false
}

// ── Pass 2: Heap Segment Analysis ───────────────────────────────────────────

func (p *hprofParser) pass2(cancel <-chan struct{}, progress ProgressCallback) error {
	totalSegs := len(p.segOffsets)
	topHeap := newTopNHeap(200) // track top 200 objects by shallow size

	for segIdx, segPos := range p.segOffsets {
		select {
		case <-cancel:
			return fmt.Errorf("canceled")
		default:
		}

		if progress != nil {
			pct := float64(segIdx+1) / float64(totalSegs) * 100
			progress("Pass 2: Analyzing heap…", math.Round(pct), fmt.Sprintf("segment %d/%d", segIdx+1, totalSegs))
		}

		tag := p.data[segPos]
		_ = tag
		bodyLen := int64(binary.BigEndian.Uint32(p.data[segPos+5 : segPos+9]))
		bodyStart := segPos + 9
		bodyEnd := bodyStart + bodyLen

		pos := bodyStart
		for pos < bodyEnd {
			if pos >= p.fileSize {
				break
			}
			subTag := p.data[pos]
			pos++

			switch subTag {
			case heapGCRootUnknown:
				pos = p.parseGCRoot(pos, "Unknown")
			case heapGCRootJNIGlobal:
				pos = p.parseGCRootJNIGlobal(pos)
			case heapGCRootJNILocal:
				pos = p.parseGCRootJNILocal(pos)
			case heapGCRootJavaFrame:
				pos = p.parseGCRootJavaFrame(pos)
			case heapGCRootNativeStack:
				pos = p.parseGCRootNativeStack(pos)
			case heapGCRootStickyClass:
				pos = p.parseGCRoot(pos, "Sticky Class")
			case heapGCRootThreadBlock:
				pos = p.parseGCRootThreadBlock(pos)
			case heapGCRootMonitor:
				pos = p.parseGCRoot(pos, "Monitor Used")
			case heapGCRootThreadObj:
				pos = p.parseGCRootThreadObj(pos)

			case heapClassDump:
				pos = p.parseClassDump(pos)

			case heapInstanceDump:
				pos = p.parseInstanceDump(pos, topHeap)

			case heapObjectArrayDump:
				pos = p.parseObjectArrayDump(pos, topHeap)

			case heapPrimArrayDump:
				pos = p.parsePrimArrayDump(pos, topHeap)

			default:
				// Unknown sub-tag — can't continue this segment safely
				pos = bodyEnd
			}
		}
	}

	p.topObjects = topHeap.toSlice()
	return nil
}

// ── GC Root Parsers ─────────────────────────────────────────────────────────

func (p *hprofParser) addGCRoot(typeName string, objectID uint64) {
	p.gcRoots[typeName]++
	entries := p.gcRootEntries[typeName]
	if len(entries) < 20 {
		className := "?"
		if cn, ok := p.classes[objectID]; ok {
			className = cn
		}
		p.gcRootEntries[typeName] = append(entries, HprofGCRootEntry{
			ObjectID:  formatID(objectID),
			ClassName: className,
		})
	}
	// Phase 2: track GC root objects for object graph edges
	if p.gcRootObjIDs != nil {
		if _, exists := p.gcRootObjIDs[objectID]; !exists {
			p.gcRootObjIDs[objectID] = typeName
		}
	}
}

func (p *hprofParser) parseGCRoot(pos int64, typeName string) int64 {
	if pos+int64(p.idSize) > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot(typeName, objectID)
	return pos + int64(p.idSize)
}

func (p *hprofParser) parseGCRootJNIGlobal(pos int64) int64 {
	// objectID + JNI global ref ID
	if pos+int64(p.idSize*2) > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("JNI Global", objectID)
	return pos + int64(p.idSize*2)
}

func (p *hprofParser) parseGCRootJNILocal(pos int64) int64 {
	// objectID + threadSerial(4) + frameNumber(4)
	if pos+int64(p.idSize)+8 > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("JNI Local", objectID)
	return pos + int64(p.idSize) + 8
}

func (p *hprofParser) parseGCRootJavaFrame(pos int64) int64 {
	// objectID + threadSerial(4) + frameNumber(4)
	if pos+int64(p.idSize)+8 > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("Java Frame", objectID)
	return pos + int64(p.idSize) + 8
}

func (p *hprofParser) parseGCRootNativeStack(pos int64) int64 {
	// objectID + threadSerial(4)
	if pos+int64(p.idSize)+4 > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("Native Stack", objectID)
	return pos + int64(p.idSize) + 4
}

func (p *hprofParser) parseGCRootThreadBlock(pos int64) int64 {
	// objectID + threadSerial(4)
	if pos+int64(p.idSize)+4 > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("Thread Block", objectID)
	return pos + int64(p.idSize) + 4
}

func (p *hprofParser) parseGCRootThreadObj(pos int64) int64 {
	// objectID + threadSerial(4) + stackTraceSerial(4)
	if pos+int64(p.idSize)+8 > p.fileSize {
		return p.fileSize
	}
	objectID := p.readID(pos)
	p.addGCRoot("Thread Object", objectID)

	// Phase 6: capture thread metadata for thread-to-heap attribution
	threadSerial := binary.BigEndian.Uint32(p.data[pos+int64(p.idSize) : pos+int64(p.idSize)+4])
	stackTraceSerial := binary.BigEndian.Uint32(p.data[pos+int64(p.idSize)+4 : pos+int64(p.idSize)+8])
	p.threadObjects[objectID] = ThreadObjectMeta{
		ThreadSerial:     threadSerial,
		StackTraceSerial: stackTraceSerial,
	}

	return pos + int64(p.idSize) + 8
}

// ── CLASS_DUMP Parser ───────────────────────────────────────────────────────

func (p *hprofParser) parseClassDump(pos int64) int64 {
	start := pos
	idSz := int64(p.idSize)

	// classObjID, stackTraceSerial(4), superClassID, classLoaderID, signersID, protDomID, reserved1, reserved2
	minLen := idSz + 4 + idSz*6
	if pos+minLen+4 > p.fileSize {
		return p.fileSize
	}

	classObjID := p.readID(pos)
	pos += idSz
	pos += 4 // stackTraceSerial
	superClassID := p.readID(pos)
	pos += idSz
	classLoaderID := p.readID(pos) // Phase 6: capture class loader
	pos += idSz
	pos += idSz * 3 // signersID, protDomID, reserved1
	pos += idSz     // reserved2

	// instanceSize(4)
	if pos+4 > p.fileSize {
		return p.fileSize
	}
	instanceSize := int(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	pos += 4

	// Constant pool: u16 count, then [u16 cpIndex, u8 type, value]
	if pos+2 > p.fileSize {
		return p.fileSize
	}
	cpCount := int(binary.BigEndian.Uint16(p.data[pos : pos+2]))
	pos += 2
	for i := 0; i < cpCount; i++ {
		if pos+3 > p.fileSize {
			return p.fileSize
		}
		pos += 2 // cpIndex
		typ := p.data[pos]
		pos++
		pos += int64(p.valueSize(typ))
	}

	// Static fields: u16 count, then [nameID(idSize), u8 type, value]
	if pos+2 > p.fileSize {
		return p.fileSize
	}
	sfCount := int(binary.BigEndian.Uint16(p.data[pos : pos+2]))
	pos += 2
	staticFields := make([]fieldDesc, 0, sfCount)
	for i := 0; i < sfCount; i++ {
		if pos+idSz+1 > p.fileSize {
			return p.fileSize
		}
		nameID := p.readID(pos)
		pos += idSz
		typ := p.data[pos]
		pos++
		pos += int64(p.valueSize(typ))
		staticFields = append(staticFields, fieldDesc{nameID: nameID, typ: typ})
	}

	// Instance fields: u16 count, then [nameID(idSize), u8 type]
	if pos+2 > p.fileSize {
		return p.fileSize
	}
	ifCount := int(binary.BigEndian.Uint16(p.data[pos : pos+2]))
	pos += 2
	instanceFields := make([]fieldDesc, 0, ifCount)
	for i := 0; i < ifCount; i++ {
		if pos+idSz+1 > p.fileSize {
			return p.fileSize
		}
		nameID := p.readID(pos)
		pos += idSz
		typ := p.data[pos]
		pos++
		instanceFields = append(instanceFields, fieldDesc{nameID: nameID, typ: typ})
	}

	p.classDumps[classObjID] = &classDump{
		superClassID:   superClassID,
		instanceSize:   instanceSize,
		staticFields:   staticFields,
		instanceFields: instanceFields,
	}

	// Phase 6: record class → class loader mapping
	if classLoaderID != 0 {
		p.classLoaderMap[classObjID] = classLoaderID
	}

	_ = start
	return pos
}

// ── INSTANCE_DUMP Parser ────────────────────────────────────────────────────

func (p *hprofParser) parseInstanceDump(pos int64, top *topNHeap) int64 {
	subRecPos := pos - 1 // position of sub-tag byte
	idSz := int64(p.idSize)
	// objectID(idSize) + stackTraceSerial(4) + classID(idSize) + numBytes(4) + bytes
	if pos+idSz+4+idSz+4 > p.fileSize {
		return p.fileSize
	}

	objectID := p.readID(pos)
	pos += idSz
	stackSerial := binary.BigEndian.Uint32(p.data[pos : pos+4]) // Phase 4: capture stack trace serial
	pos += 4 // stackTraceSerial
	classID := p.readID(pos)
	pos += idSz
	numBytes := int64(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	pos += 4
	fieldDataPos := pos // save for Phase 2 reference extraction

	shallow := idSz + 4 + idSz + 4 + numBytes // total record size contribution for shallow size
	// Use instance size as a better measure (object header + fields)
	if cd, ok := p.classDumps[classID]; ok {
		shallow = int64(cd.instanceSize) + 16 // 16 bytes object header estimate
	} else {
		shallow = numBytes + 16
	}

	p.totalObjects++
	p.totalShallowBytes += shallow

	cs := p.classSummary[classID]
	if cs == nil {
		cs = &classStats{}
		p.classSummary[classID] = cs
	}
	cs.count++
	cs.shallowTotal += shallow

	top.push(topObject{objectID: objectID, classID: classID, shallowSize: shallow, kind: "instance"})

	// Phase 2: record object and collect references
	recLen := int32(1 + idSz + 4 + idSz + 4 + numBytes)
	p.recordObject(objectID, classID, shallow, subRecPos, recLen, 1, 0)
	p.collectInstanceRefs(fieldDataPos, classID)

	// Phase 4: record allocation stack trace
	if stackSerial != 0 {
		p.objectStackSerials[objectID] = stackSerial
	}

	pos += numBytes
	return pos
}

// ── OBJECT_ARRAY_DUMP Parser ────────────────────────────────────────────────

func (p *hprofParser) parseObjectArrayDump(pos int64, top *topNHeap) int64 {
	subRecPos := pos - 1
	idSz := int64(p.idSize)
	// objectID(idSize) + stackTraceSerial(4) + numElements(4) + arrayClassID(idSize) + elements[numElements * idSize]
	if pos+idSz+4+4+idSz > p.fileSize {
		return p.fileSize
	}

	objectID := p.readID(pos)
	pos += idSz
	oaStackSerial := binary.BigEndian.Uint32(p.data[pos : pos+4]) // Phase 4
	pos += 4 // stackTraceSerial
	numElems := int64(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	pos += 4
	classID := p.readID(pos)
	pos += idSz
	elemsStart := pos // save for Phase 2 reference extraction

	shallow := 16 + numElems*idSz // header + refs

	p.totalObjects++
	p.totalShallowBytes += shallow

	cs := p.classSummary[classID]
	if cs == nil {
		cs = &classStats{}
		p.classSummary[classID] = cs
	}
	cs.count++
	cs.shallowTotal += shallow

	top.push(topObject{objectID: objectID, classID: classID, shallowSize: shallow, kind: "obj-array"})

	// Phase 2: record object and collect array element references
	recLen := int32(1 + idSz + 4 + 4 + idSz + numElems*idSz)
	p.recordObject(objectID, classID, shallow, subRecPos, recLen, 2, 0)
	p.collectArrayRefs(elemsStart, numElems)

	// Phase 4: record allocation stack trace
	if oaStackSerial != 0 {
		p.objectStackSerials[objectID] = oaStackSerial
	}

	pos += numElems * idSz
	return pos
}

// ── PRIMITIVE_ARRAY_DUMP Parser ─────────────────────────────────────────────

func (p *hprofParser) parsePrimArrayDump(pos int64, top *topNHeap) int64 {
	subRecPos := pos - 1
	idSz := int64(p.idSize)
	// objectID(idSize) + stackTraceSerial(4) + numElements(4) + elementType(1) + elements[numElements * elemSize]
	if pos+idSz+4+4+1 > p.fileSize {
		return p.fileSize
	}

	objectID := p.readID(pos)
	pos += idSz
	paStackSerial := binary.BigEndian.Uint32(p.data[pos : pos+4]) // Phase 4
	pos += 4 // stackTraceSerial
	numElems := int64(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	pos += 4
	elemType := p.data[pos]
	pos++

	elemSize := int64(primSizes[elemType])
	if elemType == 2 {
		elemSize = idSz
	}
	shallow := 16 + numElems*elemSize // header + data

	p.totalObjects++
	p.totalShallowBytes += shallow

	// Use a synthetic class ID for primitive arrays based on element type
	primClassID := uint64(0xFFFF0000) | uint64(elemType)
	cs := p.classSummary[primClassID]
	if cs == nil {
		cs = &classStats{}
		p.classSummary[primClassID] = cs
	}
	cs.count++
	cs.shallowTotal += shallow

	top.push(topObject{objectID: objectID, classID: primClassID, shallowSize: shallow, kind: "prim-array", elemType: elemType})

	// Phase 2: record object (no references from primitive arrays)
	recLen := int32(1 + idSz + 4 + 4 + 1 + numElems*elemSize)
	p.recordObject(objectID, primClassID, shallow, subRecPos, recLen, 3, elemType)

	// Phase 4: record allocation stack trace
	if paStackSerial != 0 {
		p.objectStackSerials[objectID] = paStackSerial
	}

	pos += numElems * elemSize
	return pos
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func (p *hprofParser) readID(pos int64) uint64 {
	if p.idSize == 4 {
		return uint64(binary.BigEndian.Uint32(p.data[pos : pos+4]))
	}
	return binary.BigEndian.Uint64(p.data[pos : pos+8])
}

func (p *hprofParser) valueSize(typ byte) int {
	if typ == 2 { // object reference
		return p.idSize
	}
	if s, ok := primSizes[typ]; ok {
		return s
	}
	return p.idSize // fallback
}

func (p *hprofParser) totalGCRoots() int {
	total := 0
	for _, c := range p.gcRoots {
		total += c
	}
	return total
}

func formatID(id uint64) string {
	return fmt.Sprintf("0x%x", id)
}

// javaClassName converts "java/lang/String" → "java.lang.String"
func javaClassName(name string) string {
	if strings.HasPrefix(name, "[") {
		return name // array descriptors stay as-is
	}
	return strings.ReplaceAll(name, "/", ".")
}

// primArrayClassName returns the display name for a primitive array type.
func primArrayClassName(elemType byte) string {
	names := map[byte]string{
		4: "boolean[]", 5: "char[]", 6: "float[]", 7: "double[]",
		8: "byte[]", 9: "short[]", 10: "int[]", 11: "long[]",
	}
	if n, ok := names[elemType]; ok {
		return n
	}
	return fmt.Sprintf("prim[%d][]", elemType)
}

// ── Result Builders ─────────────────────────────────────────────────────────

func (p *hprofParser) buildClassSummary() []HprofClassSummary {
	rows := make([]HprofClassSummary, 0, len(p.classSummary))
	for classID, cs := range p.classSummary {
		className := p.classes[classID]
		if className == "" {
			// Check if it's a synthetic primitive array class
			if classID&0xFFFF0000 == 0xFFFF0000 {
				className = primArrayClassName(byte(classID & 0xFF))
			} else {
				className = formatID(classID)
			}
		}
		rows = append(rows, HprofClassSummary{
			ClassID:       formatID(classID),
			ClassName:     className,
			InstanceCount: cs.count,
			ShallowSize:   cs.shallowTotal,
			RetainedSize:  cs.shallowTotal, // Phase 1: retained ≈ shallow (full retained requires object graph in Phase 2)
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].RetainedSize > rows[j].RetainedSize })
	return rows
}

func (p *hprofParser) buildGCRootGroups() []HprofGCRootGroup {
	groups := make([]HprofGCRootGroup, 0, len(p.gcRoots))
	for typeName, count := range p.gcRoots {
		groups = append(groups, HprofGCRootGroup{
			Type:      typeName,
			Count:     count,
			ObjectIDs: p.gcRootEntries[typeName],
		})
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Count > groups[j].Count })
	return groups
}

func (p *hprofParser) buildTopObjects() []TopObjectEntry {
	entries := make([]TopObjectEntry, 0, len(p.topObjects))
	for _, o := range p.topObjects {
		className := p.classes[o.classID]
		if className == "" {
			if o.kind == "prim-array" {
				className = primArrayClassName(o.elemType)
			} else if o.kind == "obj-array" {
				className = "Object[]"
			} else {
				className = formatID(o.classID)
			}
		}
		entries = append(entries, TopObjectEntry{
			ObjectID:    formatID(o.objectID),
			ClassName:   className,
			ClassID:     formatID(o.classID),
			ShallowSize: o.shallowSize,
			Kind:        o.kind,
			ElemType:    o.elemType,
		})
	}
	return entries
}

func (p *hprofParser) buildDominatorTree() []HprofDomNode {
	nodes := make([]HprofDomNode, 0, len(p.classSummary))
	for classID, cs := range p.classSummary {
		className := p.classes[classID]
		if className == "" {
			if classID&0xFFFF0000 == 0xFFFF0000 {
				className = primArrayClassName(byte(classID & 0xFF))
			} else {
				className = formatID(classID)
			}
		}
		nodes = append(nodes, HprofDomNode{
			ClassName:    className,
			ClassID:      formatID(classID),
			RetainedSize: cs.shallowTotal, // Phase 1: retained ≈ shallow
			ShallowSize:  cs.shallowTotal,
			ObjectCount:  cs.count,
		})
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].RetainedSize > nodes[j].RetainedSize })
	return nodes
}

// ── Insights Builder ────────────────────────────────────────────────────────

func (p *hprofParser) buildInsights(res *HprofResult) []HprofInsight {
	var cards []HprofInsight

	topClasses := res.TopClasses(50)
	totalRetained := int64(0)
	for _, c := range topClasses {
		totalRetained += c.RetainedSize
	}

	// 1) Heap Concentration
	if len(topClasses) > 0 {
		top1Pct := pctF(topClasses[0].RetainedSize, totalRetained)
		top5 := topClasses
		if len(top5) > 5 {
			top5 = top5[:5]
		}
		top5Retained := int64(0)
		for _, c := range top5 {
			top5Retained += c.RetainedSize
		}
		top5Pct := pctF(top5Retained, totalRetained)

		severity := "low"
		if top1Pct >= 35 || top5Pct >= 70 {
			severity = "high"
		} else if top1Pct >= 25 || top5Pct >= 55 {
			severity = "medium"
		}

		rows := make([]map[string]interface{}, 0, 10)
		limit := 10
		if limit > len(topClasses) {
			limit = len(topClasses)
		}
		for _, c := range topClasses[:limit] {
			rows = append(rows, map[string]interface{}{
				"classId":      c.ClassID,
				"className":    c.ClassName,
				"count":        c.InstanceCount,
				"shallowSize":  c.ShallowSize,
				"retainedSize": c.RetainedSize,
				"retainedPct":  roundF(pctF(c.RetainedSize, totalRetained), 1),
			})
		}

		var actions []HprofAction
		if len(topClasses) > 0 {
			actions = []HprofAction{
				{Label: fmt.Sprintf("Open %s in Histogram", topClasses[0].ClassName), TargetView: "histogram", ClassID: topClasses[0].ClassID, ClassName: topClasses[0].ClassName},
				{Label: fmt.Sprintf("Inspect %s in Dominator", topClasses[0].ClassName), TargetView: "dominator", ClassID: topClasses[0].ClassID, ClassName: topClasses[0].ClassName},
			}
		}

		cards = append(cards, HprofInsight{
			ID:       "heap-concentration",
			Title:    "Heap Concentration",
			Severity: severity,
			Summary:  fmt.Sprintf("Top classes retain %.1f%% of total retained heap.", top5Pct),
			Metrics: []HprofMetric{
				{Label: "Top 1 retained", Value: fmt.Sprintf("%.1f%%", top1Pct)},
				{Label: "Top 5 retained", Value: fmt.Sprintf("%.1f%%", top5Pct)},
				{Label: "Unique classes", Value: fmtInt64(int64(len(res.ClassSummary)))},
			},
			Rows:    rows,
			Actions: actions,
		})
	}

	// 2) Leak Suspects
	{
		type scored struct {
			HprofClassSummary
			retainedPct float64
			score       float64
		}
		var candidates []scored
		maxCount := int64(1)
		for _, c := range res.ClassSummary {
			if c.InstanceCount > maxCount {
				maxCount = c.InstanceCount
			}
		}
		for _, c := range res.ClassSummary {
			if c.InstanceCount < 100 || c.RetainedSize <= 0 {
				continue
			}
			retPct := pctF(c.RetainedSize, totalRetained)
			countPct := pctF(c.InstanceCount, maxCount)
			avgShallow := float64(0)
			if c.InstanceCount > 0 {
				avgShallow = float64(c.ShallowSize) / float64(c.InstanceCount)
			}
			growthProxy := math.Min(100, (float64(c.InstanceCount)*avgShallow)/math.Max(1, float64(res.TotalShallowBytes))*100*4)
			sc := 0.6*retPct + 0.25*countPct + 0.15*growthProxy
			candidates = append(candidates, scored{c, roundF(retPct, 1), roundF(sc, 1)})
		}
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].score > candidates[j].score })
		limit := 10
		if limit > len(candidates) {
			limit = len(candidates)
		}
		top := candidates[:limit]

		rows := make([]map[string]interface{}, 0, len(top))
		for _, c := range top {
			rows = append(rows, map[string]interface{}{
				"classId":      c.ClassID,
				"className":    c.ClassName,
				"count":        c.InstanceCount,
				"shallowSize":  c.ShallowSize,
				"retainedSize": c.RetainedSize,
				"retainedPct":  c.retainedPct,
				"score":        c.score,
			})
		}

		topScore := float64(0)
		if len(top) > 0 {
			topScore = top[0].score
		}
		severity := "low"
		if topScore >= 45 {
			severity = "high"
		} else if topScore >= 28 {
			severity = "medium"
		}

		summary := "No high-confidence leak suspects met the current thresholds."
		if len(top) > 0 {
			summary = fmt.Sprintf("%d classes match leak-suspect thresholds; top score is %.1f.", len(top), topScore)
		}

		var actions []HprofAction
		if len(top) > 0 {
			actions = []HprofAction{
				{Label: fmt.Sprintf("Open %s in Instances", top[0].ClassName), TargetView: "instances", ClassID: top[0].ClassID, ClassName: top[0].ClassName},
				{Label: fmt.Sprintf("Inspect %s in Dominator", top[0].ClassName), TargetView: "dominator", ClassID: top[0].ClassID, ClassName: top[0].ClassName},
			}
		}

		cards = append(cards, HprofInsight{
			ID:       "leak-suspects",
			Title:    "Leak Suspects",
			Severity: severity,
			Summary:  summary,
			Metrics: []HprofMetric{
				{Label: "Candidates", Value: fmtInt64(int64(len(candidates)))},
				{Label: "Top score", Value: fmt.Sprintf("%.1f", topScore)},
				{Label: "Threshold", Value: "count >= 100"},
			},
			Rows:    rows,
			Actions: actions,
		})
	}

	// 3) GC Root Pressure
	{
		totalRoots := 0
		for _, g := range res.GCRoots {
			totalRoots += g.Count
		}

		rows := make([]map[string]interface{}, 0, len(res.GCRoots))
		limit := 10
		if limit > len(res.GCRoots) {
			limit = len(res.GCRoots)
		}
		for _, g := range res.GCRoots[:limit] {
			rows = append(rows, map[string]interface{}{
				"type":  g.Type,
				"count": g.Count,
				"pct":   roundF(pctF(int64(g.Count), int64(totalRoots)), 1),
			})
		}

		jni := 0
		thread := 0
		topType := "—"
		if len(res.GCRoots) > 0 {
			topType = res.GCRoots[0].Type
		}
		for _, g := range res.GCRoots {
			if g.Type == "JNI Global" {
				jni = g.Count
			}
			if g.Type == "Thread Object" {
				thread = g.Count
			}
		}
		pressurePct := pctF(int64(jni+thread), int64(totalRoots))
		severity := "low"
		if pressurePct >= 45 {
			severity = "high"
		} else if pressurePct >= 30 {
			severity = "medium"
		}

		var actions []HprofAction
		if topType != "—" {
			actions = []HprofAction{
				{Label: fmt.Sprintf("Filter GC Roots: %s", topType), TargetView: "gcroots", RootType: topType},
			}
		}

		cards = append(cards, HprofInsight{
			ID:       "gc-root-pressure",
			Title:    "GC Root Pressure",
			Severity: severity,
			Summary:  fmt.Sprintf("%.1f%% of roots are JNI Global or Thread Object.", pressurePct),
			Metrics: []HprofMetric{
				{Label: "Total roots", Value: fmtInt64(int64(totalRoots))},
				{Label: "Top root type", Value: topType},
				{Label: "JNI + Thread", Value: fmt.Sprintf("%.1f%%", pressurePct)},
			},
			Rows:    rows,
			Actions: actions,
		})
	}

	// 4) Large Objects
	{
		rows := make([]map[string]interface{}, 0, 20)
		limit := 20
		if limit > len(p.topObjects) {
			limit = len(p.topObjects)
		}
		for _, o := range p.topObjects[:limit] {
			className := p.classes[o.classID]
			if className == "" {
				if o.kind == "prim-array" {
					className = primArrayClassName(o.elemType)
				} else if o.kind == "obj-array" {
					className = "Object[]"
				} else {
					className = formatID(o.classID)
				}
			}
			rows = append(rows, map[string]interface{}{
				"objectId":    formatID(o.objectID),
				"className":   className,
				"classId":     formatID(o.classID),
				"shallowSize": o.shallowSize,
				"kind":        o.kind,
			})
		}

		largest := int64(0)
		if len(p.topObjects) > 0 {
			largest = p.topObjects[0].shallowSize
		}
		largestPct := pctF(largest, res.TotalShallowBytes)
		severity := "low"
		if largestPct >= 10 {
			severity = "high"
		} else if largestPct >= 5 {
			severity = "medium"
		}

		summary := "No objects available for large-object analysis."
		if len(rows) > 0 {
			summary = fmt.Sprintf("Largest object is %.1f%% of total shallow heap.", largestPct)
		}

		var actions []HprofAction
		if len(rows) > 0 {
			cn := rows[0]["className"].(string)
			actions = []HprofAction{
				{Label: fmt.Sprintf("Find %s in Top Objects", cn), TargetView: "topobjects", ClassName: cn},
			}
		}

		cards = append(cards, HprofInsight{
			ID:       "large-objects",
			Title:    "Large Objects",
			Severity: severity,
			Summary:  summary,
			Metrics: []HprofMetric{
				{Label: "Largest object", Value: fmtBytesGo(largest)},
				{Label: "Largest % heap", Value: fmt.Sprintf("%.1f%%", largestPct)},
				{Label: "Retained/object", Value: "Available in Phase 2 (Object Dominator)"},
			},
			Rows:    rows,
			Actions: actions,
		})
	}

	return cards
}

// ── Numeric Helpers ─────────────────────────────────────────────────────────

func pctF(v, total int64) float64 {
	if total <= 0 {
		return 0
	}
	r := float64(v) / float64(total) * 100
	if r < 0 {
		return 0
	}
	if r > 100 {
		return 100
	}
	return r
}

func roundF(v float64, decimals int) float64 {
	pow := math.Pow(10, float64(decimals))
	return math.Round(v*pow) / pow
}

func fmtInt64(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return fmt.Sprintf("%d", n) // Go's formatting doesn't add commas; keep simple
}

func fmtBytesGo(n int64) string {
	if n < 0 {
		return "—"
	}
	if n == 0 {
		return "0 B"
	}
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1048576 {
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	if n < 1073741824 {
		return fmt.Sprintf("%.2f MB", float64(n)/1048576)
	}
	return fmt.Sprintf("%.2f GB", float64(n)/1073741824)
}

// ── Top-N Heap (min-heap tracking largest objects) ──────────────────────────

type topNHeap struct {
	items []topObject
	cap   int
}

func newTopNHeap(cap int) *topNHeap {
	return &topNHeap{items: make([]topObject, 0, cap+1), cap: cap}
}

func (h *topNHeap) push(obj topObject) {
	if len(h.items) < h.cap {
		h.items = append(h.items, obj)
		h.siftUp(len(h.items) - 1)
	} else if obj.shallowSize > h.items[0].shallowSize {
		h.items[0] = obj
		h.siftDown(0)
	}
}

func (h *topNHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[i].shallowSize >= h.items[parent].shallowSize {
			break
		}
		h.items[i], h.items[parent] = h.items[parent], h.items[i]
		i = parent
	}
}

func (h *topNHeap) siftDown(i int) {
	n := len(h.items)
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2
		if left < n && h.items[left].shallowSize < h.items[smallest].shallowSize {
			smallest = left
		}
		if right < n && h.items[right].shallowSize < h.items[smallest].shallowSize {
			smallest = right
		}
		if smallest == i {
			break
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}

func (h *topNHeap) toSlice() []topObject {
	result := make([]topObject, len(h.items))
	copy(result, h.items)
	sort.Slice(result, func(i, j int) bool { return result[i].shallowSize > result[j].shallowSize })
	return result
}

// ── Phase 2: Object Graph Collection ───────────────────────────────────────

// recordObject adds an object to the graph collection arrays.
func (p *hprofParser) recordObject(objectID, classID uint64, shallow int64, fileOffset int64, recLen int32, kind byte, elemType byte) {
	idx := p.nodeCount
	p.nodeCount++
	p.objectIDs = append(p.objectIDs, objectID)
	p.objectClassIDs = append(p.objectClassIDs, classID)
	p.objectShallows = append(p.objectShallows, shallow)
	p.objectFileOffs = append(p.objectFileOffs, fileOffset)
	p.objectRecLens = append(p.objectRecLens, recLen)
	p.objectKinds = append(p.objectKinds, kind)
	p.objectElemTypes = append(p.objectElemTypes, elemType)
	p.idToIndex[objectID] = idx
}

// getFieldLayout returns the ordered field descriptors for a class (cached, superclass-first).
func (p *hprofParser) getFieldLayout(classID uint64) []fieldDesc {
	if cached, ok := p.fieldLayoutCache[classID]; ok {
		return cached
	}
	var allFields []fieldDesc
	cid := classID
	visited := make(map[uint64]bool)
	for cid != 0 && !visited[cid] {
		visited[cid] = true
		cd := p.classDumps[cid]
		if cd == nil {
			break
		}
		// Prepend: superclass fields come first in memory layout
		allFields = append(cd.instanceFields, allFields...)
		cid = cd.superClassID
	}
	p.fieldLayoutCache[classID] = allFields
	return allFields
}

// collectInstanceRefs extracts object references from an instance's field data.
func (p *hprofParser) collectInstanceRefs(fieldDataPos int64, classID uint64) {
	srcIdx := p.nodeCount - 1 // the object we just recorded
	fields := p.getFieldLayout(classID)
	pos := fieldDataPos
	idSz := int64(p.idSize)

	for _, f := range fields {
		fSize := int64(p.valueSize(f.typ))
		if f.typ == 2 { // object reference
			if pos+idSz <= p.fileSize {
				refID := p.readID(pos)
				if refID != 0 {
					p.edgeSrcIdx = append(p.edgeSrcIdx, srcIdx)
					p.edgeDstObjID = append(p.edgeDstObjID, refID)
				}
			}
		}
		pos += fSize
	}
}

// collectArrayRefs extracts object references from an object array's elements.
func (p *hprofParser) collectArrayRefs(elemsStart int64, numElems int64) {
	srcIdx := p.nodeCount - 1
	idSz := int64(p.idSize)
	pos := elemsStart

	for i := int64(0); i < numElems; i++ {
		if pos+idSz > p.fileSize {
			break
		}
		refID := p.readID(pos)
		if refID != 0 {
			p.edgeSrcIdx = append(p.edgeSrcIdx, srcIdx)
			p.edgeDstObjID = append(p.edgeDstObjID, refID)
		}
		pos += idSz
	}
}

// buildObjectGraph resolves edges, builds CSR, computes dominator tree and retained sizes.
func (p *hprofParser) buildObjectGraph(progress ProgressCallback) *ObjectGraph {
	n := int(p.nodeCount)
	if n <= 1 {
		return nil // no objects collected
	}

	// Build edges from virtual root (0) to GC root objects
	var edgePairs [][2]int32
	gcRootTypes := make(map[int32]string)
	for objID, rootType := range p.gcRootObjIDs {
		if idx, ok := p.idToIndex[objID]; ok {
			edgePairs = append(edgePairs, [2]int32{0, idx})
			gcRootTypes[idx] = rootType
		}
	}

	if progress != nil {
		progress("Building object graph…", 20, fmt.Sprintf("resolving %d references", len(p.edgeSrcIdx)))
	}

	// Resolve raw edges (srcIndex, dstObjectID) → (srcIndex, dstIndex)
	for i := range p.edgeSrcIdx {
		if dstIdx, ok := p.idToIndex[p.edgeDstObjID[i]]; ok {
			edgePairs = append(edgePairs, [2]int32{p.edgeSrcIdx[i], dstIdx})
		}
	}
	// Free raw edge storage
	p.edgeSrcIdx = nil
	p.edgeDstObjID = nil

	if progress != nil {
		progress("Building CSR graph…", 40, fmt.Sprintf("%d nodes, %d edges", n, len(edgePairs)))
	}

	fwd := BuildCSR(n, edgePairs)
	rev := ReverseCSR(fwd)
	edgePairs = nil // free

	if progress != nil {
		progress("Computing dominator tree…", 60, "")
	}

	idom, rpoOrder := ComputeDominators(fwd, rev)

	if progress != nil {
		progress("Computing retained sizes…", 80, "")
	}

	retainedSizes := ComputeRetainedSizes(idom, p.objectShallows, rpoOrder)
	domChildren, hasChildren := BuildDomChildren(idom, retainedSizes)

	// Build class-to-nodes map
	classToNodes := make(map[uint64][]int32)
	for i := int32(1); i < p.nodeCount; i++ {
		cid := p.objectClassIDs[i]
		classToNodes[cid] = append(classToNodes[cid], i)
	}

	// Copy class names
	classNames := make(map[uint64]string, len(p.classes))
	for k, v := range p.classes {
		classNames[k] = v
	}
	// Add synthetic prim array class names
	for cid := range p.classSummary {
		if cid&0xFFFF0000 == 0xFFFF0000 {
			classNames[cid] = primArrayClassName(byte(cid & 0xFF))
		}
	}

	// Build class metadata for field decoding
	classMetas := make(map[uint64]*ObjGraphClassMeta, len(p.classDumps))
	for cid, cd := range p.classDumps {
		fields := make([]ObjGraphFieldMeta, len(cd.instanceFields))
		for i, f := range cd.instanceFields {
			name := p.strings[f.nameID]
			if name == "" {
				name = fmt.Sprintf("field_%d", i)
			}
			fields[i] = ObjGraphFieldMeta{Name: name, Type: f.typ}
		}
		classMetas[cid] = &ObjGraphClassMeta{
			SuperClassID:   cd.superClassID,
			InstanceFields: fields,
		}
	}

	if progress != nil {
		progress("Object graph complete", 100, fmt.Sprintf("%d nodes, dominator tree built", n))
	}

	return &ObjectGraph{
		NodeCount:     n,
		ObjectIDs:     p.objectIDs,
		ClassIDs:      p.objectClassIDs,
		ShallowSizes:  p.objectShallows,
		RetainedSizes: retainedSizes,
		FileOffsets:   p.objectFileOffs,
		RecordLens:    p.objectRecLens,
		Kinds:         p.objectKinds,
		ElemTypes:     p.objectElemTypes,
		IDSize:        p.idSize,
		Idom:          idom,
		FwdCSR:        fwd,
		RevCSR:        rev,
		IDToIndex:     p.idToIndex,
		ClassToNodes:  classToNodes,
		ClassNames:    classNames,
		ClassMetas:    classMetas,
		DomChildren:   domChildren,
		HasChildren:   hasChildren,
		GCRootTypes:   gcRootTypes,
	}
}

// buildClassSummaryWithRetained rebuilds class summary using accurate per-object retained sizes.
func (p *hprofParser) buildClassSummaryWithRetained(og *ObjectGraph) []HprofClassSummary {
	type classAgg struct {
		count    int64
		shallow  int64
		retained int64
	}
	agg := make(map[uint64]*classAgg)

	for i := int32(1); i < int32(og.NodeCount); i++ {
		cid := og.ClassIDs[i]
		a := agg[cid]
		if a == nil {
			a = &classAgg{}
			agg[cid] = a
		}
		a.count++
		a.shallow += og.ShallowSizes[i]
		a.retained += og.RetainedSizes[i]
	}

	rows := make([]HprofClassSummary, 0, len(agg))
	for classID, a := range agg {
		className := og.ClassNames[classID]
		if className == "" {
			className = formatID(classID)
		}
		rows = append(rows, HprofClassSummary{
			ClassID:       formatID(classID),
			ClassName:     className,
			InstanceCount: a.count,
			ShallowSize:   a.shallow,
			RetainedSize:  a.retained,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].RetainedSize > rows[j].RetainedSize })
	return rows
}

// ── Phase 4: Allocation Site Index ─────────────────────────────────────────

// buildAllocSites aggregates per-stackTraceSerial allocation data from pass2.
func (p *hprofParser) buildAllocSites() []AllocSite {
	if len(p.objectStackSerials) == 0 {
		return nil
	}

	// Aggregate: stackSerial → (shallowSum, count)
	type agg struct {
		shallow int64
		count   int64
	}
	m := make(map[uint32]*agg)
	for objID, serial := range p.objectStackSerials {
		a := m[serial]
		if a == nil {
			a = &agg{}
			m[serial] = a
		}
		// Look up shallow size from idToIndex
		if idx, ok := p.idToIndex[objID]; ok {
			a.shallow += p.objectShallows[idx]
		}
		a.count++
	}

	sites := make([]AllocSite, 0, len(m))
	for serial, a := range m {
		sites = append(sites, AllocSite{
			StackSerial: serial,
			ShallowSum:  a.shallow,
			Count:       a.count,
		})
	}
	// Sort by shallow sum descending for deterministic output
	sort.Slice(sites, func(i, j int) bool { return sites[i].ShallowSum > sites[j].ShallowSum })

	// Free the per-object map — no longer needed after aggregation
	p.objectStackSerials = nil

	return sites
}
