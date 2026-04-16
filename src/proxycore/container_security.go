package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

// ── Security audit endpoint (Phase 5B) ─────────────────────────────────────

// handleSecurityAudit inspects all containers and extracts their security
// profiles (privileged mode, capabilities, seccomp, apparmor, etc.).
func (s *Server) handleSecurityAudit(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := s.resolveContainerAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// List all containers (all states)
	containers, err := adapter.ListContainers(nil)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Inspect each container to extract security profile
	profiles := make([]SecurityProfile, 0, len(containers))
	for _, c := range containers {
		detail, err := adapter.InspectContainer(c.ID)
		if err != nil {
			// Skip containers that can't be inspected (e.g. being removed)
			continue
		}
		profiles = append(profiles, extractSecurityProfile(detail))
	}

	// Get system info for runtime security mode
	var runtimeMode string
	var secOpts []string
	sysInfo, err := adapter.SystemInfo()
	if err == nil && sysInfo != nil {
		if sysInfo.Rootless {
			runtimeMode = "rootless"
		} else {
			runtimeMode = "rootful"
		}
		secOpts = sysInfo.SecurityOptions
	}

	audit := &SecurityAudit{
		Profiles:        profiles,
		RuntimeMode:     runtimeMode,
		SecurityOptions: secOpts,
	}

	json.NewEncoder(w).Encode(ContainerResponse{SecurityAudit: audit, DurationMs: ms(t0)})
}

// extractSecurityProfile extracts security-relevant fields from a
// container's inspect data (ContainerDetail).
func extractSecurityProfile(detail *ContainerDetail) SecurityProfile {
	profile := SecurityProfile{
		ContainerID:   detail.ID,
		ContainerName: detail.Name,
		Image:         detail.Image,
		State:         detail.State,
		Privileged:    detail.Privileged,
		ExposedPorts:  detail.Ports,
		Runtime:       detail.Runtime,
	}

	// Determine if running as root
	user := strings.TrimSpace(detail.User)
	profile.RunAsRoot = user == "" || user == "0" || user == "root"

	// Extract from raw inspect JSON (HostConfig and other fields)
	if detail.Raw != nil {
		if hostConfig, ok := detail.Raw["HostConfig"].(map[string]any); ok {
			profile.ReadOnlyFS, _ = hostConfig["ReadonlyRootfs"].(bool)
			profile.PidMode = getStr(hostConfig, "PidMode")
			profile.NetworkMode = getStr(hostConfig, "NetworkMode")
			profile.IpcMode = getStr(hostConfig, "IpcMode")
			profile.UsernsMode = getStr(hostConfig, "UsernsMode")

			// Capabilities
			profile.CapAdd = getStrSlice(hostConfig, "CapAdd")
			profile.CapDrop = getStrSlice(hostConfig, "CapDrop")

			// Security options (seccomp, apparmor, selinux)
			securityOpt := getStrSlice(hostConfig, "SecurityOpt")
			for _, opt := range securityOpt {
				if strings.HasPrefix(opt, "seccomp=") || strings.HasPrefix(opt, "seccomp:") {
					profile.SeccompProfile = strings.SplitN(opt, "=", 2)[1]
					if profile.SeccompProfile == "" {
						profile.SeccompProfile = strings.SplitN(opt, ":", 2)[1]
					}
				}
				if strings.HasPrefix(opt, "apparmor=") || strings.HasPrefix(opt, "apparmor:") {
					profile.AppArmorProfile = strings.SplitN(opt, "=", 2)[1]
					if profile.AppArmorProfile == "" {
						profile.AppArmorProfile = strings.SplitN(opt, ":", 2)[1]
					}
				}
				if strings.HasPrefix(opt, "label=") || strings.HasPrefix(opt, "label:") {
					profile.SELinuxLabel = strings.SplitN(opt, "=", 2)[1]
					if profile.SELinuxLabel == "" {
						profile.SELinuxLabel = strings.SplitN(opt, ":", 2)[1]
					}
				}
			}
		}

		// Also check AppArmorProfile at top level (Docker puts it here)
		if profile.AppArmorProfile == "" {
			if aap := getStr(detail.Raw, "AppArmorProfile"); aap != "" {
				profile.AppArmorProfile = aap
			}
		}
	}

	return profile
}

// getStrSlice extracts a []string from a map[string]any (JSON arrays of strings).
func getStrSlice(m map[string]any, key string) []string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			result = append(result, s)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// ── Vulnerability scan endpoints (Phase 5B) ────────────────────────────────

// handleVulnScanners detects which vulnerability scanners are available.
func (s *Server) handleVulnScanners(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	scanners := detectVulnScanners()
	json.NewEncoder(w).Encode(ContainerResponse{VulnScanners: scanners, DurationMs: ms(t0)})
}

// handleVulnScan runs a vulnerability scan on a specific image using the
// first available scanner.
func (s *Server) handleVulnScan(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	imageRef := req.Ref
	if imageRef == "" {
		imageRef = req.ID
	}
	if imageRef == "" {
		imageRef = req.Name
	}
	if imageRef == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "image reference is required (ref, id, or name)", DurationMs: ms(t0)})
		return
	}

	// Try scanners in preference order: grype, docker scout, trivy
	scanners := detectVulnScanners()
	var result *VulnScanResult

	for _, sc := range scanners {
		if !sc.Available {
			continue
		}
		switch sc.Scanner {
		case "grype":
			result = runGrypeScan(imageRef)
		case "docker-scout":
			result = runDockerScoutScan(imageRef)
		case "trivy":
			result = runTrivyScan(imageRef)
		}
		if result != nil {
			break
		}
	}

	if result == nil {
		result = &VulnScanResult{
			Available: false,
			ImageRef:  imageRef,
			Error:     "no vulnerability scanner available (install grype, trivy, or docker scout)",
		}
	}

	json.NewEncoder(w).Encode(ContainerResponse{VulnScan: result, DurationMs: ms(t0)})
}

// detectVulnScanners checks for available vulnerability scanning tools.
func detectVulnScanners() []VulnScanResult {
	scanners := []VulnScanResult{
		{Scanner: "grype"},
		{Scanner: "docker-scout"},
		{Scanner: "trivy"},
	}

	for i := range scanners {
		switch scanners[i].Scanner {
		case "grype":
			_, err := exec.LookPath("grype")
			scanners[i].Available = err == nil
		case "docker-scout":
			// docker scout is a docker CLI plugin
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			out, err := exec.CommandContext(ctx, "docker", "scout", "version").CombinedOutput()
			cancel()
			scanners[i].Available = err == nil && len(out) > 0
		case "trivy":
			_, err := exec.LookPath("trivy")
			scanners[i].Available = err == nil
		}
	}

	return scanners
}

// runGrypeScan runs grype on an image and parses JSON output.
func runGrypeScan(imageRef string) *VulnScanResult {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "grype", imageRef, "-o", "json", "--quiet")
	out, err := cmd.Output()
	if err != nil {
		return &VulnScanResult{
			Scanner:   "grype",
			Available: true,
			ImageRef:  imageRef,
			Error:     "grype scan failed: " + err.Error(),
		}
	}

	return parseGrypeOutput(imageRef, out)
}

func parseGrypeOutput(imageRef string, data []byte) *VulnScanResult {
	var raw struct {
		Matches []struct {
			Vulnerability struct {
				ID          string `json:"id"`
				Severity    string `json:"severity"`
				Description string `json:"description"`
				Fix         struct {
					Versions []string `json:"versions"`
				} `json:"fix"`
			} `json:"vulnerability"`
			Artifact struct {
				Name    string `json:"name"`
				Version string `json:"version"`
			} `json:"artifact"`
		} `json:"matches"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return &VulnScanResult{
			Scanner:   "grype",
			Available: true,
			ImageRef:  imageRef,
			Error:     "failed to parse grype output: " + err.Error(),
		}
	}

	summary := &VulnSummary{}
	vulns := make([]VulnEntry, 0, len(raw.Matches))

	for _, m := range raw.Matches {
		sev := strings.ToLower(m.Vulnerability.Severity)
		switch sev {
		case "critical":
			summary.Critical++
		case "high":
			summary.High++
		case "medium":
			summary.Medium++
		case "low", "negligible":
			summary.Low++
		}
		summary.Total++

		fixedIn := ""
		if len(m.Vulnerability.Fix.Versions) > 0 {
			fixedIn = m.Vulnerability.Fix.Versions[0]
		}

		vulns = append(vulns, VulnEntry{
			ID:          m.Vulnerability.ID,
			Severity:    sev,
			Package:     m.Artifact.Name,
			Version:     m.Artifact.Version,
			FixedIn:     fixedIn,
			Description: m.Vulnerability.Description,
		})
	}

	return &VulnScanResult{
		Scanner:   "grype",
		Available: true,
		ImageRef:  imageRef,
		Summary:   summary,
		Vulns:     vulns,
	}
}

// runDockerScoutScan runs docker scout cves on an image.
func runDockerScoutScan(imageRef string) *VulnScanResult {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// docker scout cves --format sarif gives structured output
	cmd := exec.CommandContext(ctx, "docker", "scout", "cves", imageRef, "--format", "sarif")
	out, err := cmd.Output()
	if err != nil {
		// Fallback: try text output and parse summary
		return runDockerScoutTextFallback(ctx, imageRef)
	}

	return parseDockerScoutSarif(imageRef, out)
}

func parseDockerScoutSarif(imageRef string, data []byte) *VulnScanResult {
	// SARIF format has runs[0].results[] with level and message
	var raw struct {
		Runs []struct {
			Results []struct {
				RuleID  string `json:"ruleId"`
				Level   string `json:"level"` // error|warning|note
				Message struct {
					Text string `json:"text"`
				} `json:"message"`
			} `json:"results"`
		} `json:"runs"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return &VulnScanResult{
			Scanner:   "docker-scout",
			Available: true,
			ImageRef:  imageRef,
			Error:     "failed to parse scout output",
		}
	}

	summary := &VulnSummary{}
	vulns := make([]VulnEntry, 0)

	if len(raw.Runs) > 0 {
		for _, r := range raw.Runs[0].Results {
			sev := sarifLevelToSeverity(r.Level)
			switch sev {
			case "critical":
				summary.Critical++
			case "high":
				summary.High++
			case "medium":
				summary.Medium++
			case "low":
				summary.Low++
			}
			summary.Total++

			vulns = append(vulns, VulnEntry{
				ID:          r.RuleID,
				Severity:    sev,
				Description: r.Message.Text,
			})
		}
	}

	return &VulnScanResult{
		Scanner:   "docker-scout",
		Available: true,
		ImageRef:  imageRef,
		Summary:   summary,
		Vulns:     vulns,
	}
}

func sarifLevelToSeverity(level string) string {
	switch level {
	case "error":
		return "critical"
	case "warning":
		return "high"
	case "note":
		return "medium"
	default:
		return "low"
	}
}

func runDockerScoutTextFallback(_ context.Context, imageRef string) *VulnScanResult {
	// If SARIF fails, just report that scout is available but scan had issues
	return &VulnScanResult{
		Scanner:   "docker-scout",
		Available: true,
		ImageRef:  imageRef,
		Error:     "docker scout scan failed — try running 'docker scout cves " + imageRef + "' manually",
	}
}

// runTrivyScan runs trivy on an image and parses JSON output.
func runTrivyScan(imageRef string) *VulnScanResult {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "trivy", "image", "--format", "json", "--quiet", imageRef)
	out, err := cmd.Output()
	if err != nil {
		return &VulnScanResult{
			Scanner:   "trivy",
			Available: true,
			ImageRef:  imageRef,
			Error:     "trivy scan failed: " + err.Error(),
		}
	}

	return parseTrivyOutput(imageRef, out)
}

func parseTrivyOutput(imageRef string, data []byte) *VulnScanResult {
	var raw struct {
		Results []struct {
			Vulnerabilities []struct {
				VulnerabilityID string `json:"VulnerabilityID"`
				PkgName         string `json:"PkgName"`
				InstalledVersion string `json:"InstalledVersion"`
				FixedVersion     string `json:"FixedVersion"`
				Severity         string `json:"Severity"`
				Description      string `json:"Description"`
			} `json:"Vulnerabilities"`
		} `json:"Results"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return &VulnScanResult{
			Scanner:   "trivy",
			Available: true,
			ImageRef:  imageRef,
			Error:     "failed to parse trivy output: " + err.Error(),
		}
	}

	summary := &VulnSummary{}
	vulns := make([]VulnEntry, 0)

	for _, result := range raw.Results {
		for _, v := range result.Vulnerabilities {
			sev := strings.ToLower(v.Severity)
			switch sev {
			case "critical":
				summary.Critical++
			case "high":
				summary.High++
			case "medium":
				summary.Medium++
			case "low":
				summary.Low++
			}
			summary.Total++

			vulns = append(vulns, VulnEntry{
				ID:          v.VulnerabilityID,
				Severity:    sev,
				Package:     v.PkgName,
				Version:     v.InstalledVersion,
				FixedIn:     v.FixedVersion,
				Description: truncateDesc(v.Description, 200),
			})
		}
	}

	return &VulnScanResult{
		Scanner:   "trivy",
		Available: true,
		ImageRef:  imageRef,
		Summary:   summary,
		Vulns:     vulns,
	}
}

// truncateDesc truncates a description to maxLen characters.
func truncateDesc(s string, maxLen int) string {
	// Split on newlines and take first line only
	scanner := bufio.NewScanner(strings.NewReader(s))
	if scanner.Scan() {
		s = scanner.Text()
	}
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}
