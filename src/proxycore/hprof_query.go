package proxycore

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ── OQL (Object Query Language) Engine — Phase 4 ───────────────────────────
//
// Supports a MAT-compatible subset:
//
//   SELECT o.@objectId, o.@className, o.@retainedSize
//   FROM java.util.HashMap o
//   WHERE o.@retainedSize > 1048576
//   ORDER BY o.@retainedSize DESC
//   LIMIT 50
//
// Meta-properties: @objectId, @className, @retainedSize, @shallowSize, @classId

// OQLQuery is the parsed representation of an OQL query.
type OQLQuery struct {
	SelectCols []oqlColumn // empty = SELECT *
	FromClass  string      // fully qualified class name (or "*" for all classes)
	Alias      string      // the alias used in FROM clause (e.g., "o")
	Where      *oqlCond    // nil = no filter
	OrderBy    string      // meta-property name (without @)
	OrderDesc  bool
	Limit      int
	Offset     int
}

type oqlColumn struct {
	MetaProp string // e.g. "objectId", "className", "retainedSize", "shallowSize", "classId"
}

type oqlCondOp int

const (
	oqlOpGT oqlCondOp = iota
	oqlOpGTE
	oqlOpLT
	oqlOpLTE
	oqlOpEQ
	oqlOpNE
	oqlOpLike
	oqlOpAnd
	oqlOpOr
)

type oqlCond struct {
	// Leaf condition
	MetaProp string // e.g. "retainedSize"
	Op       oqlCondOp
	ValueNum float64
	ValueStr string
	IsStr    bool

	// Compound condition
	Left  *oqlCond
	Right *oqlCond
}

// OQLResult holds the query execution result.
type OQLResult struct {
	Columns []string                 `json:"columns"`
	Rows    []map[string]interface{} `json:"rows"`
	Total   int                      `json:"total"`
	Error   string                   `json:"error,omitempty"`
}

// ── Parser ─────────────────────────────────────────────────────────────────

var (
	oqlSelectRe = regexp.MustCompile(`(?i)^\s*SELECT\s+(.+?)\s+FROM\s+`)
	oqlFromRe   = regexp.MustCompile(`(?i)\bFROM\s+([\w.*\[\]]+)(?:\s+(\w+))?\s*`)
	oqlWhereRe  = regexp.MustCompile(`(?i)\bWHERE\s+(.+?)(?:\s+ORDER\s|\s+LIMIT\s|$)`)
	oqlOrderRe  = regexp.MustCompile(`(?i)\bORDER\s+BY\s+\w+\.@?(\w+)(?:\s+(ASC|DESC))?\s*`)
	oqlLimitRe  = regexp.MustCompile(`(?i)\bLIMIT\s+(\d+)`)
	oqlOffsetRe = regexp.MustCompile(`(?i)\bOFFSET\s+(\d+)`)
	oqlCondRe   = regexp.MustCompile(`(?i)\w+\.@?(\w+)\s*(>=|<=|!=|>|<|=|LIKE)\s*(.+)`)
)

// ParseOQL parses an OQL query string into an OQLQuery.
func ParseOQL(raw string) (*OQLQuery, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty query")
	}

	q := &OQLQuery{Limit: 100} // default limit

	// Parse SELECT
	if m := oqlSelectRe.FindStringSubmatch(raw); m != nil {
		selectPart := strings.TrimSpace(m[1])
		if selectPart == "*" {
			// all columns
		} else {
			for _, col := range strings.Split(selectPart, ",") {
				col = strings.TrimSpace(col)
				// strip alias prefix: "o.@retainedSize" → "retainedSize"
				if idx := strings.LastIndex(col, ".@"); idx >= 0 {
					col = col[idx+2:]
				} else if idx := strings.LastIndex(col, "."); idx >= 0 {
					col = col[idx+1:]
				}
				col = strings.TrimPrefix(col, "@")
				if col != "" {
					q.SelectCols = append(q.SelectCols, oqlColumn{MetaProp: col})
				}
			}
		}
	} else if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(raw)), "SELECT") {
		return nil, fmt.Errorf("query must start with SELECT")
	}

	// Parse FROM
	if m := oqlFromRe.FindStringSubmatch(raw); m != nil {
		q.FromClass = strings.TrimSpace(m[1])
		if len(m) > 2 && m[2] != "" {
			q.Alias = m[2]
		}
	} else {
		return nil, fmt.Errorf("missing FROM clause")
	}

	// Parse WHERE
	if m := oqlWhereRe.FindStringSubmatch(raw); m != nil {
		cond, err := parseOQLCondition(strings.TrimSpace(m[1]))
		if err != nil {
			return nil, fmt.Errorf("WHERE parse error: %w", err)
		}
		q.Where = cond
	}

	// Parse ORDER BY
	if m := oqlOrderRe.FindStringSubmatch(raw); m != nil {
		q.OrderBy = m[1]
		if len(m) > 2 && strings.EqualFold(m[2], "DESC") {
			q.OrderDesc = true
		}
	}

	// Parse LIMIT
	if m := oqlLimitRe.FindStringSubmatch(raw); m != nil {
		q.Limit, _ = strconv.Atoi(m[1])
		if q.Limit <= 0 {
			q.Limit = 100
		}
		if q.Limit > 10000 {
			q.Limit = 10000
		}
	}

	// Parse OFFSET
	if m := oqlOffsetRe.FindStringSubmatch(raw); m != nil {
		q.Offset, _ = strconv.Atoi(m[1])
	}

	return q, nil
}

func parseOQLCondition(s string) (*oqlCond, error) {
	s = strings.TrimSpace(s)

	// Check for AND/OR (simple single-level split)
	if parts := splitCondOp(s, " AND "); len(parts) > 1 {
		left, err := parseOQLCondition(parts[0])
		if err != nil {
			return nil, err
		}
		right, err := parseOQLCondition(strings.Join(parts[1:], " AND "))
		if err != nil {
			return nil, err
		}
		return &oqlCond{Op: oqlOpAnd, Left: left, Right: right}, nil
	}
	if parts := splitCondOp(s, " OR "); len(parts) > 1 {
		left, err := parseOQLCondition(parts[0])
		if err != nil {
			return nil, err
		}
		right, err := parseOQLCondition(strings.Join(parts[1:], " OR "))
		if err != nil {
			return nil, err
		}
		return &oqlCond{Op: oqlOpOr, Left: left, Right: right}, nil
	}

	// Leaf condition
	m := oqlCondRe.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("cannot parse condition: %s", s)
	}

	prop := m[1]
	opStr := strings.ToUpper(m[2])
	valStr := strings.TrimSpace(m[3])

	var op oqlCondOp
	switch opStr {
	case ">":
		op = oqlOpGT
	case ">=":
		op = oqlOpGTE
	case "<":
		op = oqlOpLT
	case "<=":
		op = oqlOpLTE
	case "=":
		op = oqlOpEQ
	case "!=":
		op = oqlOpNE
	case "LIKE":
		op = oqlOpLike
	default:
		return nil, fmt.Errorf("unknown operator: %s", opStr)
	}

	cond := &oqlCond{MetaProp: prop, Op: op}

	// Try numeric first
	if v, err := strconv.ParseFloat(valStr, 64); err == nil {
		cond.ValueNum = v
	} else {
		// String value — strip quotes
		cond.IsStr = true
		cond.ValueStr = strings.Trim(valStr, "'\"")
	}

	return cond, nil
}

// splitCondOp splits a string by a logical operator, case-insensitive.
func splitCondOp(s, op string) []string {
	upper := strings.ToUpper(s)
	idx := strings.Index(upper, strings.ToUpper(op))
	if idx < 0 {
		return []string{s}
	}
	return []string{s[:idx], s[idx+len(op):]}
}

// ── Executor ───────────────────────────────────────────────────────────────

// ExecuteOQL runs a parsed OQL query against the object graph.
func ExecuteOQL(q *OQLQuery, result *HprofResult) *OQLResult {
	og := result.ObjectGraph
	if og == nil {
		return &OQLResult{Error: "object graph not available"}
	}

	// Determine which classes to scan
	var targetClassIDs []uint64
	if q.FromClass == "*" {
		// All classes
		for cid := range og.ClassToNodes {
			targetClassIDs = append(targetClassIDs, cid)
		}
	} else {
		for cid, name := range og.ClassNames {
			if matchClassName(name, q.FromClass) {
				targetClassIDs = append(targetClassIDs, cid)
			}
		}
	}

	if len(targetClassIDs) == 0 {
		return &OQLResult{
			Columns: defaultOQLColumns(q),
			Rows:    []map[string]interface{}{},
			Total:   0,
		}
	}

	// Collect matching rows
	var matchedRows []oqlRow

	for _, cid := range targetClassIDs {
		nodes := og.ClassToNodes[cid]
		for _, idx := range nodes {
			view := oqlObjView{
				ObjectID:     formatID(og.ObjectIDs[idx]),
				ClassName:    og.ClassNames[og.ClassIDs[idx]],
				ClassID:      formatID(og.ClassIDs[idx]),
				ShallowSize:  og.ShallowSizes[idx],
				RetainedSize: og.RetainedSizes[idx],
			}
			if view.ClassName == "" {
				view.ClassName = view.ClassID
			}

			if q.Where != nil && !evalOQLCond(q.Where, view) {
				continue
			}
			matchedRows = append(matchedRows, oqlRow{idx: idx, obj: view})
		}
	}

	total := len(matchedRows)

	// Sort
	if q.OrderBy != "" {
		sortOQLRows(matchedRows, q.OrderBy, q.OrderDesc)
	}

	// Pagination
	offset := q.Offset
	if offset > total {
		offset = total
	}
	end := offset + q.Limit
	if end > total {
		end = total
	}
	page := matchedRows[offset:end]

	// Build result columns
	columns := defaultOQLColumns(q)

	// Build result rows
	resultRows := make([]map[string]interface{}, 0, len(page))
	for _, r := range page {
		m := make(map[string]interface{})
		for _, col := range columns {
			switch col {
			case "objectId":
				m["objectId"] = r.obj.ObjectID
			case "className":
				m["className"] = r.obj.ClassName
			case "classId":
				m["classId"] = r.obj.ClassID
			case "retainedSize":
				m["retainedSize"] = r.obj.RetainedSize
			case "shallowSize":
				m["shallowSize"] = r.obj.ShallowSize
			}
		}
		resultRows = append(resultRows, m)
	}

	return &OQLResult{
		Columns: columns,
		Rows:    resultRows,
		Total:   total,
	}
}

type oqlObjView struct {
	ObjectID     string
	ClassName    string
	ClassID      string
	ShallowSize  int64
	RetainedSize int64
}

type oqlRow struct {
	idx int32
	obj oqlObjView
}

func defaultOQLColumns(q *OQLQuery) []string {
	if len(q.SelectCols) > 0 {
		cols := make([]string, len(q.SelectCols))
		for i, c := range q.SelectCols {
			cols[i] = c.MetaProp
		}
		return cols
	}
	return []string{"objectId", "className", "shallowSize", "retainedSize"}
}

func matchClassName(actual, pattern string) bool {
	if pattern == "*" {
		return true
	}
	// Exact match
	if actual == pattern {
		return true
	}
	// Wildcard suffix: "java.util.*" matches "java.util.HashMap"
	if strings.HasSuffix(pattern, ".*") {
		prefix := strings.TrimSuffix(pattern, ".*")
		return strings.HasPrefix(actual, prefix+".")
	}
	// Wildcard prefix: "*.HashMap" matches "java.util.HashMap"
	if strings.HasPrefix(pattern, "*.") {
		suffix := strings.TrimPrefix(pattern, "*.")
		return strings.HasSuffix(actual, "."+suffix) || actual == suffix
	}
	// Simple name match: "HashMap" matches "java.util.HashMap"
	if !strings.Contains(pattern, ".") {
		parts := strings.Split(actual, ".")
		return len(parts) > 0 && parts[len(parts)-1] == pattern
	}
	return false
}

func evalOQLCond(c *oqlCond, v oqlObjView) bool {
	if c.Left != nil && c.Right != nil {
		switch c.Op {
		case oqlOpAnd:
			return evalOQLCond(c.Left, v) && evalOQLCond(c.Right, v)
		case oqlOpOr:
			return evalOQLCond(c.Left, v) || evalOQLCond(c.Right, v)
		}
	}

	// Leaf evaluation
	switch c.MetaProp {
	case "retainedSize":
		return evalNumOp(c.Op, float64(v.RetainedSize), c.ValueNum)
	case "shallowSize":
		return evalNumOp(c.Op, float64(v.ShallowSize), c.ValueNum)
	case "className":
		return evalStrOp(c.Op, v.ClassName, c.ValueStr)
	case "objectId":
		return evalStrOp(c.Op, v.ObjectID, c.ValueStr)
	case "classId":
		return evalStrOp(c.Op, v.ClassID, c.ValueStr)
	default:
		return true // unknown meta-prop → don't filter
	}
}

func evalNumOp(op oqlCondOp, actual, threshold float64) bool {
	switch op {
	case oqlOpGT:
		return actual > threshold
	case oqlOpGTE:
		return actual >= threshold
	case oqlOpLT:
		return actual < threshold
	case oqlOpLTE:
		return actual <= threshold
	case oqlOpEQ:
		return actual == threshold
	case oqlOpNE:
		return actual != threshold
	default:
		return true
	}
}

func evalStrOp(op oqlCondOp, actual, pattern string) bool {
	switch op {
	case oqlOpEQ:
		return actual == pattern
	case oqlOpNE:
		return actual != pattern
	case oqlOpLike:
		return matchLike(actual, pattern)
	default:
		return true
	}
}

func matchLike(s, pattern string) bool {
	// Simple LIKE: % = any chars, _ = single char
	// Convert to regex
	re := "^"
	for _, c := range pattern {
		switch c {
		case '%':
			re += ".*"
		case '_':
			re += "."
		default:
			re += regexp.QuoteMeta(string(c))
		}
	}
	re += "$"
	matched, err := regexp.MatchString(re, s)
	return err == nil && matched
}

func sortOQLRows(rows []oqlRow, prop string, desc bool) {
	sort.Slice(rows, func(i, j int) bool {
		var less bool
		switch prop {
		case "retainedSize":
			less = rows[i].obj.RetainedSize < rows[j].obj.RetainedSize
		case "shallowSize":
			less = rows[i].obj.ShallowSize < rows[j].obj.ShallowSize
		case "className":
			less = rows[i].obj.ClassName < rows[j].obj.ClassName
		case "objectId":
			less = rows[i].obj.ObjectID < rows[j].obj.ObjectID
		default:
			less = rows[i].obj.RetainedSize < rows[j].obj.RetainedSize
		}
		if desc {
			return !less
		}
		return less
	})
}

// ── Flame Graph Builder ────────────────────────────────────────────────────
//
// Builds an allocation flame graph from .hprof stack traces.
// Each object references a stackTraceSerial; we aggregate shallow sizes
// per unique call-chain to produce a flamegraph-compatible tree.

// FlameNode represents a node in the flame graph tree.
type FlameNode struct {
	Name     string       `json:"name"`
	Value    int64        `json:"value"` // total allocated bytes through this frame
	Self     int64        `json:"self"`  // allocated directly at this frame (leaf)
	Children []*FlameNode `json:"children,omitempty"`
}

// HprofStackFrame stores a parsed STACK_FRAME record.
type HprofStackFrame struct {
	FrameID       uint64
	MethodNameID  uint64
	MethodSigID   uint64
	SourceFileID  uint64
	ClassSerialNo uint32
	LineNumber    int32 // -1=unknown, -2=compiled, -3=native, 0+=actual line
}

// HprofStackTrace stores a parsed STACK_TRACE record.
type HprofStackTrace struct {
	Serial       uint32
	ThreadSerial uint32
	FrameIDs     []uint64
}

// AllocSite links an object's stackTraceSerial to aggregated allocation data.
type AllocSite struct {
	StackSerial uint32
	ShallowSum  int64
	Count       int64
}

// BuildFlameGraph constructs a flame graph tree from allocation site data.
func BuildFlameGraph(result *HprofResult) *FlameNode {
	if result.AllocSites == nil || len(result.AllocSites) == 0 {
		return nil
	}
	if result.StackTraces == nil || result.StackFrames == nil {
		return nil
	}

	root := &FlameNode{Name: "all allocations", Value: 0}

	for _, site := range result.AllocSites {
		trace, ok := result.StackTraces[site.StackSerial]
		if !ok || len(trace.FrameIDs) == 0 {
			// Objects without stack traces → aggregate under "(no stack trace)"
			addFlameChild(root, "(no stack trace)", site.ShallowSum)
			continue
		}

		// Walk frames bottom-up (callee last in .hprof, but flame graphs are root-down)
		node := root
		for i := len(trace.FrameIDs) - 1; i >= 0; i-- {
			fid := trace.FrameIDs[i]
			frame, ok := result.StackFrames[fid]
			if !ok {
				continue
			}
			name := resolveFrameName(result, frame)
			node = getOrCreateChild(node, name)
		}
		node.Self += site.ShallowSum
	}

	// Propagate values up
	propagateFlameValues(root)

	// Prune very small branches (< 0.1% of total)
	if root.Value > 0 {
		threshold := int64(math.Max(1, float64(root.Value)*0.001))
		pruneFlameTree(root, threshold)
	}

	return root
}

func resolveFrameName(result *HprofResult, frame HprofStackFrame) string {
	// Resolve class name from serial
	className := ""
	if cObjID, ok := result.FrameClassSerialToObjID[frame.ClassSerialNo]; ok {
		if name, ok2 := result.Strings[cObjID]; ok2 {
			className = javaClassName(name)
		}
	}
	if className == "" {
		className = fmt.Sprintf("class#%d", frame.ClassSerialNo)
	}

	// Resolve method name
	methodName := ""
	if name, ok := result.Strings[frame.MethodNameID]; ok {
		methodName = name
	}
	if methodName == "" {
		methodName = "unknown"
	}

	// Line info
	lineStr := ""
	switch frame.LineNumber {
	case -1:
		lineStr = ""
	case -2:
		lineStr = " (compiled)"
	case -3:
		lineStr = " (native)"
	default:
		if frame.LineNumber > 0 {
			lineStr = fmt.Sprintf(":%d", frame.LineNumber)
		}
	}

	return className + "." + methodName + lineStr
}

func getOrCreateChild(parent *FlameNode, name string) *FlameNode {
	for _, c := range parent.Children {
		if c.Name == name {
			return c
		}
	}
	child := &FlameNode{Name: name}
	parent.Children = append(parent.Children, child)
	return child
}

func addFlameChild(parent *FlameNode, name string, value int64) {
	child := getOrCreateChild(parent, name)
	child.Self += value
}

func propagateFlameValues(node *FlameNode) int64 {
	total := node.Self
	for _, c := range node.Children {
		total += propagateFlameValues(c)
	}
	node.Value = total
	return total
}

func pruneFlameTree(node *FlameNode, threshold int64) {
	kept := node.Children[:0]
	for _, c := range node.Children {
		if c.Value >= threshold {
			pruneFlameTree(c, threshold)
			kept = append(kept, c)
		}
	}
	node.Children = kept
}
