package proxycore

import (
	"encoding/binary"
	"sort"
)

// ── String Deduplication Analysis (Phase 3) ────────────────────────────────

// StringDedupResult holds aggregated duplicate-string analysis.
type StringDedupResult struct {
	TotalWaste  int64              `json:"totalWaste"`
	UniqueCount int                `json:"uniqueCount"`
	Entries     []StringDedupEntry `json:"entries"`
}

// StringDedupEntry represents a single unique string value with duplicates.
type StringDedupEntry struct {
	Value       string `json:"value"`
	Count       int64  `json:"count"`
	WastedBytes int64  `json:"wastedBytes"`
	SingleSize  int64  `json:"singleSize"`
}

// AnalyzeStringDedup finds duplicate java.lang.String values and quantifies waste.
// Requires a fully built ObjectGraph and mmapData.
func AnalyzeStringDedup(result *HprofResult) *StringDedupResult {
	og := result.ObjectGraph
	if og == nil || result.mmapData == nil {
		return nil
	}

	// Find the class ID(s) for java.lang.String
	var stringClassIDs []uint64
	for cid, name := range og.ClassNames {
		if name == "java.lang.String" {
			stringClassIDs = append(stringClassIDs, cid)
		}
	}
	if len(stringClassIDs) == 0 {
		return &StringDedupResult{}
	}

	// For each String instance, extract the backing array content
	type stringInfo struct {
		value string
		size  int64 // shallow size of backing array
	}
	var allStrings []stringInfo

	for _, scid := range stringClassIDs {
		nodes := og.ClassToNodes[scid]
		for _, idx := range nodes {
			val, arrSize := extractStringValue(og, result.mmapData, idx)
			if val != "" || arrSize > 0 {
				allStrings = append(allStrings, stringInfo{value: val, size: arrSize})
			}
		}
	}

	// Group by value
	type group struct {
		count int64
		size  int64 // size of one backing array
	}
	groups := make(map[string]*group)
	for _, s := range allStrings {
		g := groups[s.value]
		if g == nil {
			g = &group{size: s.size}
			groups[s.value] = g
		}
		g.count++
	}

	// Build entries for strings with duplicates
	var entries []StringDedupEntry
	totalWaste := int64(0)
	for val, g := range groups {
		if g.count <= 1 {
			continue
		}
		waste := (g.count - 1) * g.size
		totalWaste += waste
		// Truncate display value for very long strings
		displayVal := val
		if len(displayVal) > 200 {
			displayVal = displayVal[:200] + "…"
		}
		entries = append(entries, StringDedupEntry{
			Value:       displayVal,
			Count:       g.count,
			WastedBytes: waste,
			SingleSize:  g.size,
		})
	}

	// Sort by wasted bytes descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].WastedBytes > entries[j].WastedBytes
	})

	return &StringDedupResult{
		TotalWaste:  totalWaste,
		UniqueCount: len(entries),
		Entries:     entries,
	}
}

// extractStringValue reads a java.lang.String's backing char[]/byte[] content.
func extractStringValue(og *ObjectGraph, mmapData []byte, stringIdx int32) (string, int64) {
	if og.Kinds[stringIdx] != 1 { // must be instance
		return "", 0
	}

	classID := og.ClassIDs[stringIdx]
	fileOff := og.FileOffsets[stringIdx]
	recLen := int64(og.RecordLens[stringIdx])
	idSize := og.IDSize

	if fileOff < 0 || fileOff+recLen > int64(len(mmapData)) {
		return "", 0
	}

	recData := mmapData[fileOff : fileOff+recLen]

	// Instance sub-record layout:
	// tag(1) + objectID(idSize) + stackSerial(4) + classID(idSize) + numBytes(4) + [field data]
	headerLen := 1 + idSize + 4 + idSize + 4
	if headerLen >= len(recData) {
		return "", 0
	}
	fieldData := recData[headerLen:]

	// Walk field chain to find the "value" field (object reference to char[]/byte[])
	allFields := og.resolveFieldChain(classID)
	pos := 0
	var valueRefID uint64
	found := false
	for _, f := range allFields {
		fSize := ogFieldValueSize(f.Type, idSize)
		if pos+fSize > len(fieldData) {
			break
		}
		if f.Name == "value" && f.Type == 2 { // object reference
			if idSize == 4 {
				valueRefID = uint64(binary.BigEndian.Uint32(fieldData[pos : pos+4]))
			} else {
				valueRefID = binary.BigEndian.Uint64(fieldData[pos : pos+8])
			}
			found = true
			break
		}
		pos += fSize
	}
	if !found || valueRefID == 0 {
		return "", 0
	}

	// Look up the backing array
	arrIdx, ok := og.IDToIndex[valueRefID]
	if !ok || og.Kinds[arrIdx] != 3 { // must be primitive array
		return "", 0
	}

	arrOff := og.FileOffsets[arrIdx]
	arrRecLen := int64(og.RecordLens[arrIdx])
	if arrOff < 0 || arrOff+arrRecLen > int64(len(mmapData)) {
		return "", 0
	}

	arrRec := mmapData[arrOff : arrOff+arrRecLen]
	// Prim array layout: tag(1) + objectID(idSize) + stackSerial(4) + numElements(4) + elemType(1) + data
	arrHeader := 1 + idSize + 4 + 4 + 1
	if arrHeader > len(arrRec) {
		return "", 0
	}

	numElems := int(binary.BigEndian.Uint32(arrRec[1+idSize+4 : 1+idSize+4+4]))
	elemType := arrRec[1+idSize+4+4]
	dataStart := arrHeader

	arrShallow := og.ShallowSizes[arrIdx]

	switch elemType {
	case 5: // char[] (Java 8 and earlier)
		if dataStart+numElems*2 > len(arrRec) {
			numElems = (len(arrRec) - dataStart) / 2
		}
		// Limit extraction to 200 chars to avoid huge strings
		readElems := numElems
		if readElems > 200 {
			readElems = 200
		}
		runes := make([]rune, readElems)
		for i := 0; i < readElems; i++ {
			off := dataStart + i*2
			runes[i] = rune(binary.BigEndian.Uint16(arrRec[off : off+2]))
		}
		return string(runes), arrShallow

	case 8: // byte[] (Java 9+ compact strings, Latin-1)
		if dataStart+numElems > len(arrRec) {
			numElems = len(arrRec) - dataStart
		}
		readElems := numElems
		if readElems > 200 {
			readElems = 200
		}
		return string(arrRec[dataStart : dataStart+readElems]), arrShallow

	default:
		return "", 0
	}
}

// ── Collection Fill Analysis (Phase 3) ─────────────────────────────────────

// CollectionAnalysis holds aggregated collection fill analysis.
type CollectionAnalysis struct {
	Entries []CollectionEntry `json:"entries"`
}

// CollectionEntry represents a single analyzed collection instance.
type CollectionEntry struct {
	ObjectID    string  `json:"objectId"`
	ClassName   string  `json:"className"`
	Size        int64   `json:"size"`
	Capacity    int64   `json:"capacity"`
	FillRatio   float64 `json:"fillRatio"`
	WastedBytes int64   `json:"wastedBytes"`
	Category    string  `json:"category"` // "empty", "oversized", "normal"
}

// collectionClasses defines which classes we analyze and their field names for size/capacity.
var collectionClasses = map[string]struct {
	sizeField     string
	capacityField string // if empty, capacity comes from backing array
	arrayField    string // backing array field name
}{
	"java.util.HashMap":                      {sizeField: "size", arrayField: "table"},
	"java.util.LinkedHashMap":                {sizeField: "size", arrayField: "table"},
	"java.util.Hashtable":                    {sizeField: "count", arrayField: "table"},
	"java.util.ArrayList":                    {sizeField: "size", arrayField: "elementData"},
	"java.util.HashSet":                      {sizeField: "size", arrayField: "table"}, // Note: actually wraps HashMap, but we try
	"java.util.ArrayDeque":                   {sizeField: "head", arrayField: "elements"},
	"java.util.concurrent.ConcurrentHashMap": {sizeField: "sizeCtl", arrayField: "table"},
}

// AnalyzeCollections inspects collection instances for fill ratio and waste.
func AnalyzeCollections(result *HprofResult) *CollectionAnalysis {
	og := result.ObjectGraph
	if og == nil || result.mmapData == nil {
		return nil
	}

	var entries []CollectionEntry

	for className, spec := range collectionClasses {
		// Find class ID(s) matching this name
		var classIDs []uint64
		for cid, name := range og.ClassNames {
			if name == className {
				classIDs = append(classIDs, cid)
			}
		}

		for _, cid := range classIDs {
			nodes := og.ClassToNodes[cid]
			for _, idx := range nodes {
				entry := analyzeOneCollection(og, result.mmapData, idx, className, spec)
				if entry != nil {
					entries = append(entries, *entry)
				}
			}
		}
	}

	// Sort by wasted bytes descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].WastedBytes > entries[j].WastedBytes
	})

	return &CollectionAnalysis{Entries: entries}
}

func analyzeOneCollection(og *ObjectGraph, mmapData []byte, idx int32, className string, spec struct {
	sizeField     string
	capacityField string
	arrayField    string
}) *CollectionEntry {
	if og.Kinds[idx] != 1 { // must be instance
		return nil
	}

	fields := og.DecodeFields(mmapData, idx)
	if fields == nil {
		return nil
	}

	// Read size field
	size := int64(-1)
	var arrayRefID string
	for _, f := range fields {
		if f.Name == spec.sizeField {
			size = parsePrimInt64(f.Value.Raw)
		}
		if f.Name == spec.arrayField && f.Value.Kind == "ref" {
			arrayRefID = f.Value.ObjectID
		}
	}

	// Special handling for HashSet: it wraps a HashMap internally
	// If we couldn't find the fields directly, check for "map" field
	if size < 0 && className == "java.util.HashSet" {
		for _, f := range fields {
			if f.Name == "map" && f.Value.Kind == "ref" {
				// The HashSet delegates to an internal HashMap — skip this;
				// the HashMap itself will be analyzed separately
				return nil
			}
		}
	}

	// For ArrayDeque: size = (tail - head + elements.length) % elements.length
	// Simplification: use the backing array length as capacity, skip size calculation
	if className == "java.util.ArrayDeque" {
		size = -1 // we'll estimate from capacity
	}

	// Determine capacity from backing array
	capacity := int64(0)
	if arrayRefID != "" {
		arrObjID := parseHexID(arrayRefID)
		if arrIdx, ok := og.IDToIndex[arrObjID]; ok {
			capacity = getArrayLength(og, mmapData, arrIdx)
		}
	}

	if size < 0 {
		// For classes where we couldn't read size, estimate from other data
		// Conservative: treat as normal, don't report waste
		return nil
	}

	if capacity <= 0 && size > 0 {
		// No backing array found — can't determine waste
		return nil
	}

	fillRatio := float64(0)
	if capacity > 0 {
		fillRatio = float64(size) / float64(capacity)
	}

	// Estimate wasted bytes: unused slots × reference size (pointers in backing array)
	wastedSlots := capacity - size
	if wastedSlots < 0 {
		wastedSlots = 0
	}
	wastedBytes := wastedSlots * int64(og.IDSize) // each slot is a pointer

	category := "normal"
	if size == 0 && capacity > 0 {
		category = "empty"
	} else if capacity > 0 && fillRatio < 0.25 && capacity >= 16 {
		category = "oversized"
	}

	return &CollectionEntry{
		ObjectID:    formatID(og.ObjectIDs[idx]),
		ClassName:   className,
		Size:        size,
		Capacity:    capacity,
		FillRatio:   roundF(fillRatio*100, 1),
		WastedBytes: wastedBytes,
		Category:    category,
	}
}

// getArrayLength reads the numElements field from an object array's raw record.
func getArrayLength(og *ObjectGraph, mmapData []byte, arrIdx int32) int64 {
	// Must be obj-array (kind=2) or prim-array (kind=3)
	kind := og.Kinds[arrIdx]
	if kind != 2 && kind != 3 {
		return 0
	}

	off := og.FileOffsets[arrIdx]
	recLen := int64(og.RecordLens[arrIdx])
	idSize := og.IDSize

	if off < 0 || off+recLen > int64(len(mmapData)) {
		return 0
	}

	// Layout for obj-array: tag(1) + objectID(idSize) + stackSerial(4) + numElements(4) + ...
	// Layout for prim-array: tag(1) + objectID(idSize) + stackSerial(4) + numElements(4) + ...
	numElemsOff := int64(1 + idSize + 4)
	if numElemsOff+4 > recLen {
		return 0
	}

	return int64(binary.BigEndian.Uint32(mmapData[off+numElemsOff : off+numElemsOff+4]))
}

// parsePrimInt64 parses a primitive value string (from DecodedField.Value.Raw) to int64.
func parsePrimInt64(raw string) int64 {
	if raw == "" || raw == "?" {
		return -1
	}
	var v int64
	n, _ := parseIntFromString(raw)
	v = int64(n)
	return v
}

// parseIntFromString extracts an integer from a string like "42" or "-1".
func parseIntFromString(s string) (int, bool) {
	n := 0
	neg := false
	i := 0
	if i < len(s) && s[i] == '-' {
		neg = true
		i++
	}
	if i >= len(s) || s[i] < '0' || s[i] > '9' {
		return 0, false
	}
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		n = n*10 + int(s[i]-'0')
		i++
	}
	if neg {
		n = -n
	}
	return n, true
}
