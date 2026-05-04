package proxycore

import (
	"fmt"
	"sort"
)

// ── Phase 6: Class Loader Leak Detection ─────────────────────────────────────

// ClassLoaderAnalysis holds the results of class loader analysis.
type ClassLoaderAnalysis struct {
	Loaders         []ClassLoaderEntry `json:"loaders"`
	DuplicateCount  int                `json:"duplicateCount"`
	LeakWarning     string             `json:"leakWarning,omitempty"`
	TotalClassCount int                `json:"totalClassCount"`
	TotalLoaders    int                `json:"totalLoaders"`
}

// ClassLoaderEntry represents a single class loader with its loaded classes.
type ClassLoaderEntry struct {
	LoaderID     string `json:"loaderId"`
	LoaderClass  string `json:"loaderClass"`
	ClassCount   int    `json:"classCount"`
	RetainedSize int64  `json:"retainedSize"`
	IsDuplicate  bool   `json:"isDuplicate"`
	DuplicateOf  string `json:"duplicateOf,omitempty"`
}

// AnalyzeClassLoaders scans CLASS_DUMP records for class loader objects and
// detects duplicates (potential class loader leaks).
// It uses the parser's classDumps which record superClassID per class, and the
// LOAD_CLASS association of class → classLoaderID captured during Pass 2.
func AnalyzeClassLoaders(result *HprofResult) *ClassLoaderAnalysis {
	og := result.ObjectGraph
	if og == nil {
		return &ClassLoaderAnalysis{}
	}

	// classLoaderClasses: loaderObjectID → list of class names loaded by it
	loaderClasses := make(map[uint64][]string)
	// classLoaderClassID: loaderObjectID → class name of the loader itself
	loaderOwnClass := make(map[uint64]string)

	for classObjID, loaderID := range result.ClassLoaderMap {
		className := ""
		if name, ok := og.ClassNames[classObjID]; ok {
			className = name
		} else if name, ok := result.Strings[classObjID]; ok {
			className = name
		}
		if className == "" {
			className = formatID(classObjID)
		}
		loaderClasses[loaderID] = append(loaderClasses[loaderID], className)

		if _, seen := loaderOwnClass[loaderID]; !seen {
			if name, ok := og.ClassNames[loaderID]; ok {
				loaderOwnClass[loaderID] = name
			} else {
				loaderOwnClass[loaderID] = formatID(loaderID)
			}
		}
	}

	// Compute retained size for each loader (if available in object graph)
	loaderRetained := make(map[uint64]int64)
	for loaderID := range loaderClasses {
		if idx, ok := og.IDToIndex[loaderID]; ok && idx > 0 && int(idx) < len(og.RetainedSizes) {
			loaderRetained[loaderID] = og.RetainedSizes[idx]
		}
	}

	// Detect duplicates: loaders holding identical sorted class name sets
	type classSetKey string
	setToLoader := make(map[classSetKey]uint64)
	duplicateOf := make(map[uint64]uint64) // loaderID → original loaderID

	for loaderID, classes := range loaderClasses {
		sorted := make([]string, len(classes))
		copy(sorted, classes)
		sort.Strings(sorted)
		key := classSetKey("")
		for _, c := range sorted {
			key += classSetKey(c + "\x00")
		}
		if original, exists := setToLoader[key]; exists {
			duplicateOf[loaderID] = original
		} else {
			setToLoader[key] = loaderID
		}
	}

	// Build entries
	entries := make([]ClassLoaderEntry, 0, len(loaderClasses))
	totalClassCount := 0
	for loaderID, classes := range loaderClasses {
		entry := ClassLoaderEntry{
			LoaderID:     formatID(loaderID),
			LoaderClass:  loaderOwnClass[loaderID],
			ClassCount:   len(classes),
			RetainedSize: loaderRetained[loaderID],
		}
		if origID, dup := duplicateOf[loaderID]; dup {
			entry.IsDuplicate = true
			entry.DuplicateOf = formatID(origID)
		}
		entries = append(entries, entry)
		totalClassCount += len(classes)
	}

	// Sort by retained size descending
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].RetainedSize > entries[j].RetainedSize
	})

	analysis := &ClassLoaderAnalysis{
		Loaders:         entries,
		DuplicateCount:  len(duplicateOf),
		TotalClassCount: totalClassCount,
		TotalLoaders:    len(loaderClasses),
	}

	if len(duplicateOf) > 0 {
		analysis.LeakWarning = formatLeakWarning(len(duplicateOf))
	}

	return analysis
}

func formatLeakWarning(count int) string {
	if count == 1 {
		return "1 class loader holds a class set identical to another loader — potential class loader leak."
	}
	return fmt.Sprintf("%d class loaders hold identical class sets — potential class loader leak.", count)
}
