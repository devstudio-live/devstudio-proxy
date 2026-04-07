package proxycore

import (
	"encoding/binary"
	"fmt"
	"math"
	"sort"
)

// ── CSR Graph ──────────────────────────────────────────────────────────────

// CSRGraph is a Compressed Sparse Row representation of a directed graph.
type CSRGraph struct {
	NodeCount int
	Offsets   []int32 // len = NodeCount+1; adjacency of node i = Edges[Offsets[i]:Offsets[i+1]]
	Edges     []int32
}

// BuildCSR constructs a CSR graph from directed edge pairs [src, dst].
func BuildCSR(nodeCount int, pairs [][2]int32) *CSRGraph {
	offsets := make([]int32, nodeCount+1)
	for _, e := range pairs {
		if int(e[0]) < nodeCount {
			offsets[e[0]+1]++
		}
	}
	for i := 1; i <= nodeCount; i++ {
		offsets[i] += offsets[i-1]
	}
	edges := make([]int32, offsets[nodeCount])
	pos := make([]int32, nodeCount)
	copy(pos, offsets[:nodeCount])
	for _, e := range pairs {
		s := e[0]
		if int(s) < nodeCount {
			edges[pos[s]] = e[1]
			pos[s]++
		}
	}
	return &CSRGraph{NodeCount: nodeCount, Offsets: offsets, Edges: edges}
}

// ReverseCSR builds the transposed graph.
func ReverseCSR(g *CSRGraph) *CSRGraph {
	n := g.NodeCount
	offsets := make([]int32, n+1)
	for _, t := range g.Edges {
		if int(t) < n {
			offsets[t+1]++
		}
	}
	for i := 1; i <= n; i++ {
		offsets[i] += offsets[i-1]
	}
	edges := make([]int32, len(g.Edges))
	pos := make([]int32, n)
	copy(pos, offsets[:n])
	for src := 0; src < n; src++ {
		for j := g.Offsets[src]; j < g.Offsets[src+1]; j++ {
			t := g.Edges[j]
			if int(t) < n {
				edges[pos[t]] = int32(src)
				pos[t]++
			}
		}
	}
	return &CSRGraph{NodeCount: n, Offsets: offsets, Edges: edges}
}

// ── Dominator Tree (Cooper-Harvey-Kennedy iterative algorithm) ─────────────

// ComputeDominators computes immediate dominators iteratively.
// Node 0 is the root (idom[0] = 0). Unreachable nodes get idom[i] = -1.
// Returns idom array and the reverse-postorder sequence used.
func ComputeDominators(fwd, rev *CSRGraph) (idom, rpoOrder []int32) {
	n := fwd.NodeCount
	if n == 0 {
		return nil, nil
	}

	// Iterative DFS from node 0 to compute reverse postorder
	rpoNum := make([]int32, n)
	for i := range rpoNum {
		rpoNum[i] = -1
	}
	rpoOrder = make([]int32, 0, n)

	type frame struct{ node, edge int32 }
	stack := []frame{{0, fwd.Offsets[0]}}
	rpoNum[0] = -2 // visited but not yet numbered

	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		if top.edge < fwd.Offsets[top.node+1] {
			next := fwd.Edges[top.edge]
			top.edge++
			if int(next) < n && rpoNum[next] == -1 {
				rpoNum[next] = -2
				stack = append(stack, frame{next, fwd.Offsets[next]})
			}
		} else {
			rpoOrder = append(rpoOrder, top.node)
			stack = stack[:len(stack)-1]
		}
	}

	// Reverse the postorder to get reverse-postorder
	for i, j := 0, len(rpoOrder)-1; i < j; i, j = i+1, j-1 {
		rpoOrder[i], rpoOrder[j] = rpoOrder[j], rpoOrder[i]
	}
	for rpo, node := range rpoOrder {
		rpoNum[node] = int32(rpo)
	}

	// Iterative dominator computation
	idom = make([]int32, n)
	for i := range idom {
		idom[i] = -1
	}
	idom[0] = 0

	intersect := func(b1, b2 int32) int32 {
		for b1 != b2 {
			for rpoNum[b1] > rpoNum[b2] {
				b1 = idom[b1]
			}
			for rpoNum[b2] > rpoNum[b1] {
				b2 = idom[b2]
			}
		}
		return b1
	}

	for changed := true; changed; {
		changed = false
		for _, b := range rpoOrder[1:] { // skip root
			newIdom := int32(-1)
			for j := rev.Offsets[b]; j < rev.Offsets[b+1]; j++ {
				p := rev.Edges[j]
				if idom[p] != -1 {
					if newIdom == -1 {
						newIdom = p
					} else {
						newIdom = intersect(p, newIdom)
					}
				}
			}
			if newIdom != -1 && idom[b] != newIdom {
				idom[b] = newIdom
				changed = true
			}
		}
	}

	return idom, rpoOrder
}

// ComputeRetainedSizes accumulates retained sizes bottom-up on the dominator tree.
func ComputeRetainedSizes(idom []int32, shallowSizes []int64, rpoOrder []int32) []int64 {
	retained := make([]int64, len(idom))
	copy(retained, shallowSizes)
	// Process in reverse RPO order (children before parents in the dominator tree)
	for i := len(rpoOrder) - 1; i >= 1; i-- {
		node := rpoOrder[i]
		parent := idom[node]
		if parent >= 0 && parent != node {
			retained[parent] += retained[node]
		}
	}
	return retained
}

// BuildDomChildren precomputes dominator tree children sorted by retained size desc.
func BuildDomChildren(idom []int32, retainedSizes []int64) (children [][]int32, hasChildren []bool) {
	n := len(idom)
	children = make([][]int32, n)
	hasChildren = make([]bool, n)
	for i := 1; i < n; i++ {
		p := idom[i]
		if p >= 0 && p != int32(i) {
			children[p] = append(children[p], int32(i))
			hasChildren[p] = true
		}
	}
	for i := range children {
		if len(children[i]) > 1 {
			c := children[i]
			sort.Slice(c, func(a, b int) bool {
				return retainedSizes[c[a]] > retainedSizes[c[b]]
			})
		}
	}
	return
}

// ── Object Graph ───────────────────────────────────────────────────────────

// ObjectGraph holds the full per-object reference graph and dominator tree.
type ObjectGraph struct {
	NodeCount     int       // includes virtual root at index 0
	ObjectIDs     []uint64  // [0]=0 (virtual root), [1..N]=real objects
	ClassIDs      []uint64
	ShallowSizes  []int64
	RetainedSizes []int64
	FileOffsets   []int64
	RecordLens    []int32
	Kinds         []byte // 0=virtual-root, 1=instance, 2=obj-array, 3=prim-array
	ElemTypes     []byte
	IDSize        int

	Idom        []int32
	FwdCSR      *CSRGraph
	RevCSR      *CSRGraph
	IDToIndex   map[uint64]int32
	ClassToNodes map[uint64][]int32
	ClassNames  map[uint64]string
	ClassMetas  map[uint64]*ObjGraphClassMeta
	DomChildren [][]int32
	HasChildren []bool
	GCRootTypes map[int32]string // node index → root type
}

// ObjGraphClassMeta stores resolved class field metadata for field decoding.
type ObjGraphClassMeta struct {
	SuperClassID   uint64
	InstanceFields []ObjGraphFieldMeta
}

// ObjGraphFieldMeta describes a single instance field.
type ObjGraphFieldMeta struct {
	Name string
	Type byte // 2=object, 4=bool, 5=char, 6=float, 7=double, 8=byte, 9=short, 10=int, 11=long
}

// ── Retainer Paths ─────────────────────────────────────────────────────────

// RetainerNode is a node in a retainer path.
type RetainerNode struct {
	ObjectID  string `json:"objectId"`
	ClassName string `json:"className"`
	IsGcRoot  bool   `json:"isGcRoot"`
	RootType  string `json:"rootType,omitempty"`
}

// FindRetainerPaths finds up to maxPaths paths from the target object to GC roots
// via BFS on the reverse reference graph.
func FindRetainerPaths(og *ObjectGraph, targetIdx int32, maxPaths, maxDepth int) [][]RetainerNode {
	if targetIdx <= 0 || int(targetIdx) >= og.NodeCount {
		return nil
	}

	parent := make(map[int32]int32)
	parent[targetIdx] = -1

	type entry struct {
		idx   int32
		depth int
	}
	queue := []entry{{targetIdx, 0}}
	head := 0
	var paths [][]RetainerNode

	for head < len(queue) && len(paths) < maxPaths {
		cur := queue[head]
		head++
		if cur.depth >= maxDepth {
			continue
		}

		// Check referrers of the current node (reverse graph)
		for j := og.RevCSR.Offsets[cur.idx]; j < og.RevCSR.Offsets[cur.idx+1]; j++ {
			ref := og.RevCSR.Edges[j]
			if ref == 0 {
				// Current node is a GC root — build path from target to here
				path := traceRetainerPath(og, cur.idx, parent)
				paths = append(paths, path)
				if len(paths) >= maxPaths {
					break
				}
				continue
			}
			if _, visited := parent[ref]; visited {
				continue
			}
			parent[ref] = cur.idx
			queue = append(queue, entry{ref, cur.depth + 1})
		}
	}

	return paths
}

func traceRetainerPath(og *ObjectGraph, gcRootIdx int32, parent map[int32]int32) []RetainerNode {
	// Trace from GC root back to target via parent pointers, then reverse
	var indices []int32
	for cur := gcRootIdx; cur != -1; cur = parent[cur] {
		indices = append(indices, cur)
	}
	// Reverse so path goes: target → ... → GC root
	for i, j := 0, len(indices)-1; i < j; i, j = i+1, j-1 {
		indices[i], indices[j] = indices[j], indices[i]
	}

	path := make([]RetainerNode, len(indices))
	for i, idx := range indices {
		cn := og.ClassNames[og.ClassIDs[idx]]
		if cn == "" {
			cn = formatID(og.ClassIDs[idx])
		}
		path[i] = RetainerNode{
			ObjectID:  formatID(og.ObjectIDs[idx]),
			ClassName: cn,
		}
		if idx == gcRootIdx {
			path[i].IsGcRoot = true
			if rt, ok := og.GCRootTypes[idx]; ok {
				path[i].RootType = rt
			}
		}
	}
	return path
}

// ── Field Decoding ─────────────────────────────────────────────────────────

// DecodedField represents a decoded instance field.
type DecodedField struct {
	Name  string     `json:"name"`
	Type  byte       `json:"type"`
	Value FieldValue `json:"value"`
}

// FieldValue holds the decoded value of a field.
type FieldValue struct {
	Kind      string `json:"kind,omitempty"`      // "ref" for object references
	ObjectID  string `json:"objectId,omitempty"`
	ClassName string `json:"className,omitempty"`
	Raw       string `json:"raw,omitempty"`
}

// DecodeFields decodes the instance fields of an object from raw mmapped data.
func (og *ObjectGraph) DecodeFields(mmapData []byte, objectIdx int32) []DecodedField {
	if og.Kinds[objectIdx] != 1 { // only instances
		return nil
	}

	classID := og.ClassIDs[objectIdx]
	fileOff := og.FileOffsets[objectIdx]
	recLen := int64(og.RecordLens[objectIdx])
	idSize := og.IDSize

	if fileOff < 0 || fileOff+recLen > int64(len(mmapData)) {
		return nil
	}

	recData := mmapData[fileOff : fileOff+recLen]

	// Instance sub-record layout:
	// tag(1) + objectID(idSize) + stackSerial(4) + classID(idSize) + numBytes(4) + [field data]
	headerLen := 1 + idSize + 4 + idSize + 4
	if headerLen >= len(recData) {
		return nil
	}
	fieldData := recData[headerLen:]

	// Walk superclass chain to get all fields in order (super first)
	allFields := og.resolveFieldChain(classID)

	var result []DecodedField
	pos := 0
	for _, f := range allFields {
		fSize := ogFieldValueSize(f.Type, idSize)
		if pos+fSize > len(fieldData) {
			break
		}

		var value FieldValue
		if f.Type == 2 { // object reference
			var refID uint64
			if idSize == 4 {
				refID = uint64(binary.BigEndian.Uint32(fieldData[pos : pos+4]))
			} else {
				refID = binary.BigEndian.Uint64(fieldData[pos : pos+8])
			}
			if refID == 0 {
				value = FieldValue{Raw: "null"}
			} else {
				refClassName := ""
				if refIdx, ok := og.IDToIndex[refID]; ok {
					cn := og.ClassNames[og.ClassIDs[refIdx]]
					if cn != "" {
						refClassName = cn
					}
				}
				value = FieldValue{Kind: "ref", ObjectID: formatID(refID), ClassName: refClassName}
			}
		} else {
			value = FieldValue{Raw: readPrimValueGo(fieldData[pos:], f.Type)}
		}

		result = append(result, DecodedField{
			Name:  f.Name,
			Type:  f.Type,
			Value: value,
		})
		pos += fSize
	}

	return result
}

func (og *ObjectGraph) resolveFieldChain(classID uint64) []ObjGraphFieldMeta {
	var allFields []ObjGraphFieldMeta
	cid := classID
	visited := make(map[uint64]bool)
	for cid != 0 && !visited[cid] {
		visited[cid] = true
		cm := og.ClassMetas[cid]
		if cm == nil {
			break
		}
		// Prepend: superclass fields come first in memory layout
		allFields = append(cm.InstanceFields, allFields...)
		cid = cm.SuperClassID
	}
	return allFields
}

func ogFieldValueSize(typ byte, idSize int) int {
	if typ == 2 {
		return idSize
	}
	sizes := map[byte]int{4: 1, 5: 2, 6: 4, 7: 8, 8: 1, 9: 2, 10: 4, 11: 8}
	if s, ok := sizes[typ]; ok {
		return s
	}
	return idSize
}

func readPrimValueGo(data []byte, typ byte) string {
	switch typ {
	case 4: // boolean
		if data[0] != 0 {
			return "true"
		}
		return "false"
	case 5: // char
		v := binary.BigEndian.Uint16(data[:2])
		if v >= 32 && v < 127 {
			return fmt.Sprintf("'%c' (0x%x)", rune(v), v)
		}
		return fmt.Sprintf("0x%x", v)
	case 6: // float
		v := math.Float32frombits(binary.BigEndian.Uint32(data[:4]))
		return fmt.Sprintf("%g", v)
	case 7: // double
		v := math.Float64frombits(binary.BigEndian.Uint64(data[:8]))
		return fmt.Sprintf("%g", v)
	case 8: // byte
		return fmt.Sprintf("%d", int8(data[0]))
	case 9: // short
		return fmt.Sprintf("%d", int16(binary.BigEndian.Uint16(data[:2])))
	case 10: // int
		return fmt.Sprintf("%d", int32(binary.BigEndian.Uint32(data[:4])))
	case 11: // long
		return fmt.Sprintf("%d", int64(binary.BigEndian.Uint64(data[:8])))
	default:
		return "?"
	}
}
