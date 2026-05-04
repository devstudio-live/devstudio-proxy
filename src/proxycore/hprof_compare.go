package proxycore

import (
	"sort"
)

// ── Heap Diff / Comparison Engine — Phase 5 ─────────────────────────────────
//
// Compares two HprofResult structs (from two .hprof parse jobs) and produces
// a class-level delta table: count changes, retained size changes, new classes
// (only in B), and freed classes (only in A).

// HprofDiffResult holds the comparison output between two heap dumps.
type HprofDiffResult struct {
	// Delta rows: one per class that exists in A, B, or both
	Deltas []HprofClassDelta `json:"deltas"`

	// Summary stats
	TotalCountA     int64 `json:"totalCountA"`
	TotalCountB     int64 `json:"totalCountB"`
	TotalRetainedA  int64 `json:"totalRetainedA"`
	TotalRetainedB  int64 `json:"totalRetainedB"`
	NewClassCount   int   `json:"newClassCount"`
	FreedClassCount int   `json:"freedClassCount"`
}

// HprofClassDelta represents the delta for a single class between two dumps.
type HprofClassDelta struct {
	ClassName     string `json:"className"`
	ClassID       string `json:"classId"`
	CountA        int64  `json:"countA"`
	CountB        int64  `json:"countB"`
	CountDelta    int64  `json:"countDelta"`
	RetainedA     int64  `json:"retainedA"`
	RetainedB     int64  `json:"retainedB"`
	RetainedDelta int64  `json:"retainedDelta"`
	ShallowA      int64  `json:"shallowA"`
	ShallowB      int64  `json:"shallowB"`
	ShallowDelta  int64  `json:"shallowDelta"`
	Category      string `json:"category"` // "grew", "shrunk", "new", "freed", "unchanged"
}

// CompareHprofResults computes class-level deltas between result A and result B.
func CompareHprofResults(a, b *HprofResult) *HprofDiffResult {
	if a == nil || b == nil {
		return &HprofDiffResult{Deltas: []HprofClassDelta{}}
	}

	// Index A by className
	aByName := make(map[string]HprofClassSummary, len(a.ClassSummary))
	for _, cs := range a.ClassSummary {
		aByName[cs.ClassName] = cs
	}

	// Index B by className
	bByName := make(map[string]HprofClassSummary, len(b.ClassSummary))
	for _, cs := range b.ClassSummary {
		bByName[cs.ClassName] = cs
	}

	// Collect all class names from both dumps
	allNames := make(map[string]struct{}, len(aByName)+len(bByName))
	for name := range aByName {
		allNames[name] = struct{}{}
	}
	for name := range bByName {
		allNames[name] = struct{}{}
	}

	result := &HprofDiffResult{
		Deltas: make([]HprofClassDelta, 0, len(allNames)),
	}

	for name := range allNames {
		csA := aByName[name]
		csB := bByName[name]

		delta := HprofClassDelta{
			ClassName:     name,
			CountA:        csA.InstanceCount,
			CountB:        csB.InstanceCount,
			CountDelta:    csB.InstanceCount - csA.InstanceCount,
			RetainedA:     csA.RetainedSize,
			RetainedB:     csB.RetainedSize,
			RetainedDelta: csB.RetainedSize - csA.RetainedSize,
			ShallowA:      csA.ShallowSize,
			ShallowB:      csB.ShallowSize,
			ShallowDelta:  csB.ShallowSize - csA.ShallowSize,
		}

		// Use classId from whichever side has it
		if csB.ClassID != "" {
			delta.ClassID = csB.ClassID
		} else {
			delta.ClassID = csA.ClassID
		}

		// Categorize
		_, inA := aByName[name]
		_, inB := bByName[name]
		switch {
		case inA && !inB:
			delta.Category = "freed"
			result.FreedClassCount++
		case !inA && inB:
			delta.Category = "new"
			result.NewClassCount++
		case delta.RetainedDelta > 0:
			delta.Category = "grew"
		case delta.RetainedDelta < 0:
			delta.Category = "shrunk"
		default:
			delta.Category = "unchanged"
		}

		result.Deltas = append(result.Deltas, delta)

		result.TotalCountA += csA.InstanceCount
		result.TotalCountB += csB.InstanceCount
		result.TotalRetainedA += csA.RetainedSize
		result.TotalRetainedB += csB.RetainedSize
	}

	// Default sort: by absolute retained delta descending
	sort.Slice(result.Deltas, func(i, j int) bool {
		return abs64(result.Deltas[i].RetainedDelta) > abs64(result.Deltas[j].RetainedDelta)
	})

	return result
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
