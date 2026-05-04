package proxycore

import (
	"sort"
)

// ── Phase 6: Thread-to-Heap Attribution ──────────────────────────────────────

// ThreadObjectMeta holds metadata captured from GC_ROOT_THREAD_OBJ records.
type ThreadObjectMeta struct {
	ThreadSerial     uint32
	StackTraceSerial uint32
}

// ThreadRetainedAnalysis holds thread-level heap attribution data.
type ThreadRetainedAnalysis struct {
	Threads    []ThreadEntry `json:"threads"`
	TotalCount int           `json:"totalCount"`
}

// ThreadEntry represents a single thread's heap attribution.
type ThreadEntry struct {
	ThreadName   string `json:"threadName"`
	ThreadID     string `json:"threadId"`
	RetainedSize int64  `json:"retainedSize"`
	RetainedObjs int64  `json:"retainedObjects"`
	RootType     string `json:"rootType"`
}

// AnalyzeThreads maps Thread Object GC roots to their retained subtree sizes.
// Thread objects are identified by heapGCRootThreadObj records captured during
// Pass 2 into the ThreadObjects map.
func AnalyzeThreads(result *HprofResult) *ThreadRetainedAnalysis {
	og := result.ObjectGraph
	if og == nil || len(result.ThreadObjects) == 0 {
		return &ThreadRetainedAnalysis{}
	}

	entries := make([]ThreadEntry, 0, len(result.ThreadObjects))

	for objectID, threadMeta := range result.ThreadObjects {
		idx, ok := og.IDToIndex[objectID]
		if !ok || idx <= 0 {
			continue
		}

		var retainedSize int64
		var retainedObjs int64

		// Sum retained sizes of all nodes dominated by this thread object
		if int(idx) < len(og.RetainedSizes) {
			retainedSize = og.RetainedSizes[idx]
		}

		// Count objects in the dominator subtree
		retainedObjs = countDomSubtree(og, idx)

		// Resolve thread name from the object's class fields if possible
		threadName := resolveThreadName(og, result, objectID)
		if threadName == "" {
			threadName = "Thread-" + formatID(objectID)
		}

		rootType := "Thread Object"
		if rt, ok := og.GCRootTypes[idx]; ok {
			rootType = rt
		}

		entries = append(entries, ThreadEntry{
			ThreadName:   threadName,
			ThreadID:     formatID(objectID),
			RetainedSize: retainedSize,
			RetainedObjs: retainedObjs,
			RootType:     rootType,
		})

		_ = threadMeta // threadSerial/stackTraceSerial available for future use
	}

	// Sort by retained size descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RetainedSize > entries[j].RetainedSize
	})

	return &ThreadRetainedAnalysis{
		Threads:    entries,
		TotalCount: len(entries),
	}
}

// countDomSubtree counts the number of nodes in the dominator subtree rooted at idx.
func countDomSubtree(og *ObjectGraph, rootIdx int32) int64 {
	if og.DomChildren == nil {
		return 1
	}
	var count int64
	stack := []int32{rootIdx}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		count++
		if int(n) < len(og.DomChildren) {
			stack = append(stack, og.DomChildren[n]...)
		}
	}
	return count
}

// resolveThreadName attempts to extract the "name" field from a java.lang.Thread instance.
// Thread.name is a java.lang.String which itself has a "value" field (char[] or byte[]).
func resolveThreadName(og *ObjectGraph, result *HprofResult, threadObjID uint64) string {
	idx, ok := og.IDToIndex[threadObjID]
	if !ok || idx <= 0 {
		return ""
	}

	classID := og.ClassIDs[idx]
	className := og.ClassNames[classID]
	if className != "java.lang.Thread" && className != "" {
		// Could be a subclass — walk the class hierarchy
		// For simplicity, only handle java.lang.Thread directly
		// The thread name field is inherited from Thread, so subclasses also have it
	}

	// Look for a "name" field by reading the instance data
	if result.mmapData == nil || og.Kinds[idx] != 1 { // 1 = instance
		return ""
	}

	fileOff := og.FileOffsets[idx]
	if fileOff <= 0 || fileOff >= int64(len(result.mmapData)) {
		return ""
	}

	// Instance dump layout: tag(1) + objectID(idSize) + stackSerial(4) + classID(idSize) + numBytes(4) + fieldData
	idSz := int64(og.IDSize)
	fieldDataStart := fileOff + 1 + idSz + 4 + idSz + 4

	// Walk the class hierarchy to find the "name" field offset
	nameFieldOff, nameFieldFound := findFieldOffset(og, result, classID, "name", fieldDataStart)
	if !nameFieldFound {
		return ""
	}

	// The "name" field is an object reference (to a String)
	if nameFieldOff+idSz > int64(len(result.mmapData)) {
		return ""
	}

	var stringObjID uint64
	if og.IDSize == 4 {
		stringObjID = uint64(readUint32BE(result.mmapData, nameFieldOff))
	} else {
		stringObjID = readUint64BE(result.mmapData, nameFieldOff)
	}

	if stringObjID == 0 {
		return ""
	}

	return extractStringByObjID(og, result, stringObjID)
}

// findFieldOffset walks the class hierarchy to find the byte offset of a named field
// within the instance data. Returns the absolute file offset and whether it was found.
func findFieldOffset(og *ObjectGraph, result *HprofResult, classID uint64, fieldName string, fieldDataStart int64) (int64, bool) {
	// Build field layout by walking from superclass down
	type fieldInfo struct {
		name string
		typ  byte
		size int
	}
	var allFields []fieldInfo

	visited := make(map[uint64]bool)
	chain := []uint64{classID}
	cid := classID
	for {
		meta, ok := og.ClassMetas[cid]
		if !ok {
			break
		}
		if meta.SuperClassID == 0 || visited[meta.SuperClassID] {
			break
		}
		visited[cid] = true
		cid = meta.SuperClassID
		chain = append(chain, cid)
	}

	// Walk from root superclass down
	idSz := og.IDSize
	for i := len(chain) - 1; i >= 0; i-- {
		meta, ok := og.ClassMetas[chain[i]]
		if !ok {
			continue
		}
		for _, f := range meta.InstanceFields {
			fSize := fieldTypeSize(f.Type, idSz)
			allFields = append(allFields, fieldInfo{name: f.Name, typ: f.Type, size: fSize})
		}
	}

	// Compute offset of the target field
	off := fieldDataStart
	for _, f := range allFields {
		if f.name == fieldName {
			return off, true
		}
		off += int64(f.size)
	}
	return 0, false
}

// fieldTypeSize returns the byte size of a field based on its HPROF type tag.
func fieldTypeSize(typ byte, idSize int) int {
	switch typ {
	case 2: // object reference
		return idSize
	case 4: // boolean
		return 1
	case 5: // char
		return 2
	case 6: // float
		return 4
	case 7: // double
		return 8
	case 8: // byte
		return 1
	case 9: // short
		return 2
	case 10: // int
		return 4
	case 11: // long
		return 8
	default:
		return 0
	}
}

// extractStringByObjID reads a java.lang.String object's value from the heap dump.
func extractStringByObjID(og *ObjectGraph, result *HprofResult, stringObjID uint64) string {
	idx, ok := og.IDToIndex[stringObjID]
	if !ok || idx <= 0 || og.Kinds[idx] != 1 {
		return ""
	}

	val, _ := extractStringValue(og, result.mmapData, idx)
	return val
}

// readUint32BE reads a big-endian uint32 at the given offset.
func readUint32BE(data []byte, off int64) uint32 {
	if off+4 > int64(len(data)) {
		return 0
	}
	return uint32(data[off])<<24 | uint32(data[off+1])<<16 | uint32(data[off+2])<<8 | uint32(data[off+3])
}

// readUint64BE reads a big-endian uint64 at the given offset.
func readUint64BE(data []byte, off int64) uint64 {
	if off+8 > int64(len(data)) {
		return 0
	}
	return uint64(data[off])<<56 | uint64(data[off+1])<<48 | uint64(data[off+2])<<40 | uint64(data[off+3])<<32 |
		uint64(data[off+4])<<24 | uint64(data[off+5])<<16 | uint64(data[off+6])<<8 | uint64(data[off+7])
}
