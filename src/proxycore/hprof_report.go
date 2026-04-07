package proxycore

import (
	"bytes"
	"fmt"
	"html/template"
	"sort"
	"time"
)

// ── Phase 6: HTML Report Generator ───────────────────────────────────────────

// HprofReportReq is the request body for POST /hprof/report.
type HprofReportReq struct {
	JobID     string `json:"jobId"`
	Format    string `json:"format"`    // "html" (only format for now)
	IncludeAI bool   `json:"includeAI"` // placeholder — AI summary integration
}

// HprofReportResult is the response from report generation.
type HprofReportResult struct {
	HTML string `json:"html,omitempty"`
}

// GenerateHprofReport produces an HTML report from a completed HprofResult.
func GenerateHprofReport(result *HprofResult, includeAI bool) (*HprofReportResult, error) {
	if result == nil {
		return nil, fmt.Errorf("no analysis result available")
	}

	data := buildReportData(result, includeAI)

	var buf bytes.Buffer
	tmpl, err := template.New("hprof-report").Funcs(template.FuncMap{
		"fmtBytes": reportFmtBytes,
		"fmtNum":   reportFmtNum,
		"pct":      reportPct,
		"int64":    func(v int) int64 { return int64(v) },
	}).Parse(reportTemplate)
	if err != nil {
		return nil, fmt.Errorf("template parse: %w", err)
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("template execute: %w", err)
	}

	return &HprofReportResult{HTML: buf.String()}, nil
}

// ── Report Data ─────────────────────────────────────────────────────────────

type reportData struct {
	GeneratedAt    string
	FileSize       int64
	Version        string
	TotalObjects   int64
	TotalShallow   int64
	ClassCount     int
	GCRootCount    int
	TopClasses     []HprofClassSummary
	Insights       []HprofInsight
	ClassLoaders   *ClassLoaderAnalysis
	Threads        *ThreadRetainedAnalysis
	IncludeAI      bool
}

func buildReportData(result *HprofResult, includeAI bool) reportData {
	topClasses := make([]HprofClassSummary, len(result.ClassSummary))
	copy(topClasses, result.ClassSummary)
	sort.Slice(topClasses, func(i, j int) bool { return topClasses[i].RetainedSize > topClasses[j].RetainedSize })
	if len(topClasses) > 30 {
		topClasses = topClasses[:30]
	}

	d := reportData{
		GeneratedAt:  time.Now().UTC().Format(time.RFC3339),
		FileSize:     result.FileSize,
		Version:      result.Version,
		TotalObjects: result.TotalObjects,
		TotalShallow: result.TotalShallowBytes,
		ClassCount:   result.ClassCount,
		GCRootCount:  result.GCRootCount,
		TopClasses:   topClasses,
		Insights:     result.Insights,
		IncludeAI:    includeAI,
	}

	// Phase 6 analysis results (may be nil if not yet computed)
	if result.ClassLoaders != nil {
		d.ClassLoaders = result.ClassLoaders
	}
	if result.ThreadRetained != nil {
		d.Threads = result.ThreadRetained
	}

	return d
}

// ── Formatting Helpers ──────────────────────────────────────────────────────

func reportFmtBytes(n int64) string {
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

func reportFmtNum(n int64) string {
	if n == 0 {
		return "0"
	}
	// Simple thousands separator
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}

func reportPct(v, total int64) string {
	if total == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%.1f%%", float64(v)/float64(total)*100)
}

// ── HTML Template ───────────────────────────────────────────────────────────

const reportTemplate = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Java Heap Analysis Report</title>
<style>
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
    color:#24292f; background:#f6f8fa; line-height:1.5; padding:24px; }
  .container { max-width:960px; margin:0 auto; }
  h1 { font-size:1.5rem; margin-bottom:4px; }
  h2 { font-size:1.1rem; margin:24px 0 12px; border-bottom:1px solid #d0d7de; padding-bottom:6px; }
  .meta { color:#57606a; font-size:.85rem; margin-bottom:20px; }
  .stats { display:grid; grid-template-columns:repeat(auto-fill,minmax(160px,1fr)); gap:10px; margin-bottom:20px; }
  .stat { background:#fff; border:1px solid #d0d7de; border-radius:8px; padding:12px; }
  .stat-label { font-size:.72rem; color:#57606a; margin-bottom:2px; }
  .stat-value { font-size:1.05rem; font-weight:700; }
  table { width:100%; border-collapse:collapse; font-size:.85rem; margin-bottom:16px; }
  th { background:#f6f8fa; padding:8px 10px; text-align:left; border-bottom:2px solid #d0d7de;
    font-weight:600; color:#57606a; white-space:nowrap; }
  td { padding:6px 10px; border-bottom:1px solid #d0d7de; font-family:monospace; font-size:.82rem; }
  tr:hover td { background:#f6f8fa; }
  .right { text-align:right; }
  .insight { background:#fff; border:1px solid #d0d7de; border-radius:8px; padding:12px; margin-bottom:10px; }
  .insight-title { font-weight:600; font-size:.9rem; }
  .insight-summary { font-size:.82rem; color:#57606a; margin-top:4px; }
  .sev { font-size:.7rem; font-weight:600; padding:2px 8px; border-radius:999px; text-transform:uppercase; }
  .sev-high { background:rgba(248,81,73,.12); color:#cf222e; }
  .sev-medium { background:rgba(210,153,34,.12); color:#9a6700; }
  .sev-low { background:rgba(63,185,80,.12); color:#1a7f37; }
  .warn { background:rgba(210,153,34,.08); border:1px solid rgba(210,153,34,.3);
    border-radius:8px; padding:10px 14px; font-size:.85rem; color:#9a6700; margin-bottom:12px; }
  .footer { margin-top:32px; text-align:center; font-size:.75rem; color:#57606a; }
</style>
</head>
<body>
<div class="container">
  <h1>☕ Java Heap Analysis Report</h1>
  <div class="meta">Generated {{.GeneratedAt}} · HPROF {{.Version}} · {{fmtBytes .FileSize}}</div>

  <div class="stats">
    <div class="stat"><div class="stat-label">File Size</div><div class="stat-value">{{fmtBytes .FileSize}}</div></div>
    <div class="stat"><div class="stat-label">Total Objects</div><div class="stat-value">{{fmtNum .TotalObjects}}</div></div>
    <div class="stat"><div class="stat-label">Total Shallow</div><div class="stat-value">{{fmtBytes .TotalShallow}}</div></div>
    <div class="stat"><div class="stat-label">Classes</div><div class="stat-value">{{fmtNum (int64 .ClassCount)}}</div></div>
    <div class="stat"><div class="stat-label">GC Roots</div><div class="stat-value">{{fmtNum (int64 .GCRootCount)}}</div></div>
  </div>

  {{if .Insights}}
  <h2>Triage Insights</h2>
  {{range .Insights}}
  <div class="insight">
    <div style="display:flex;align-items:center;gap:8px;">
      <span class="insight-title">{{.Title}}</span>
      <span class="sev sev-{{.Severity}}">{{.Severity}}</span>
    </div>
    <div class="insight-summary">{{.Summary}}</div>
  </div>
  {{end}}
  {{end}}

  <h2>Top Classes by Retained Size</h2>
  <table>
    <thead><tr>
      <th>Class</th>
      <th class="right">Instances</th>
      <th class="right">Shallow Size</th>
      <th class="right">Retained Size</th>
    </tr></thead>
    <tbody>
    {{range .TopClasses}}
    <tr>
      <td>{{.ClassName}}</td>
      <td class="right">{{fmtNum .InstanceCount}}</td>
      <td class="right">{{fmtBytes .ShallowSize}}</td>
      <td class="right">{{fmtBytes .RetainedSize}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>

  {{if .ClassLoaders}}
  <h2>Class Loaders</h2>
  {{if .ClassLoaders.LeakWarning}}
  <div class="warn">⚠ {{.ClassLoaders.LeakWarning}}</div>
  {{end}}
  <table>
    <thead><tr>
      <th>Class Loader</th>
      <th class="right">Loaded Classes</th>
      <th class="right">Retained Size</th>
      <th>Duplicate?</th>
    </tr></thead>
    <tbody>
    {{range .ClassLoaders.Loaders}}
    <tr>
      <td>{{.LoaderClass}}</td>
      <td class="right">{{fmtNum (int64 .ClassCount)}}</td>
      <td class="right">{{fmtBytes .RetainedSize}}</td>
      <td>{{if .IsDuplicate}}⚠ Yes{{else}}No{{end}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{end}}

  {{if .Threads}}
  <h2>Thread-to-Heap Attribution</h2>
  <table>
    <thead><tr>
      <th>Thread Name</th>
      <th class="right">Retained Size</th>
      <th class="right">Retained Objects</th>
      <th>Root Type</th>
    </tr></thead>
    <tbody>
    {{range .Threads.Threads}}
    <tr>
      <td>{{.ThreadName}}</td>
      <td class="right">{{fmtBytes .RetainedSize}}</td>
      <td class="right">{{fmtNum .RetainedObjs}}</td>
      <td>{{.RootType}}</td>
    </tr>
    {{end}}
    </tbody>
  </table>
  {{end}}

  <div class="footer">
    Generated by DevStudio Java Heap Profiler · Proxy-assisted analysis
  </div>
</div>
</body>
</html>`
