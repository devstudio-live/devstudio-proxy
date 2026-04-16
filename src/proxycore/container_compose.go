package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ── Compose project detection ──────────────────────────────────────────────
//
// Compose projects are detected by inspecting container labels set by
// Docker Compose / Podman Compose:
//
//   com.docker.compose.project          → project name
//   com.docker.compose.service          → service name
//   com.docker.compose.project.config_files → compose file path
//   com.docker.compose.project.working_dir  → project working directory
//   com.docker.compose.container-number     → replica number
//   com.docker.compose.depends_on          → dependency list (if present)

const (
	labelComposeProject    = "com.docker.compose.project"
	labelComposeService    = "com.docker.compose.service"
	labelComposeConfigFile = "com.docker.compose.project.config_files"
	labelComposeWorkingDir = "com.docker.compose.project.working_dir"
	labelComposeNumber     = "com.docker.compose.container-number"
	labelComposeDependsOn  = "com.docker.compose.depends_on"
)

// listComposeProjects groups containers by their compose project label
// and returns a list of ComposeProject summaries.
func listComposeProjects(containers []ContainerInfo) []ComposeProject {
	projectMap := map[string]*ComposeProject{}

	for _, c := range containers {
		projName := c.Labels[labelComposeProject]
		if projName == "" {
			continue
		}

		proj, ok := projectMap[projName]
		if !ok {
			proj = &ComposeProject{
				Name:       projName,
				ConfigFile: c.Labels[labelComposeConfigFile],
				WorkingDir: c.Labels[labelComposeWorkingDir],
				Created:    c.Created,
			}
			projectMap[projName] = proj
		}

		// Track earliest creation time
		if !c.Created.IsZero() && (proj.Created.IsZero() || c.Created.Before(proj.Created)) {
			proj.Created = c.Created
		}

		svcNum, _ := strconv.Atoi(c.Labels[labelComposeNumber])
		var dependsOn []string
		if deps := c.Labels[labelComposeDependsOn]; deps != "" {
			for _, d := range strings.Split(deps, ",") {
				d = strings.TrimSpace(d)
				// Format can be "service:condition" — take just the service name
				if idx := strings.Index(d, ":"); idx >= 0 {
					d = d[:idx]
				}
				if d != "" {
					dependsOn = append(dependsOn, d)
				}
			}
		}

		svc := ComposeService{
			Name:        c.Labels[labelComposeService],
			ContainerID: c.ID,
			Image:       c.Image,
			State:       c.State,
			Status:      c.Status,
			Ports:       c.Ports,
			ServiceNum:  svcNum,
			DependsOn:   dependsOn,
			Created:     c.Created,
		}

		proj.Services = append(proj.Services, svc)
		proj.Total++
		if strings.ToLower(c.State) == "running" {
			proj.Running++
		} else {
			proj.Stopped++
		}
	}

	// Build sorted result
	var projects []ComposeProject
	for _, proj := range projectMap {
		// Sort services by name for stable output
		sort.Slice(proj.Services, func(i, j int) bool {
			return proj.Services[i].Name < proj.Services[j].Name
		})
		// Derive project status
		if proj.Running == proj.Total {
			proj.Status = "running"
		} else if proj.Running == 0 {
			proj.Status = "stopped"
		} else {
			proj.Status = "partial"
		}
		projects = append(projects, *proj)
	}

	sort.Slice(projects, func(i, j int) bool {
		return projects[i].Name < projects[j].Name
	})

	return projects
}

// ── Compose file retrieval ──────────────────────────────────────────────────

// readComposeFile reads the compose file content for a project.
// It locates the file via the container label or falls back to common filenames.
func readComposeFile(project ComposeProject) (string, error) {
	configPath := project.ConfigFile
	workingDir := project.WorkingDir

	if configPath != "" {
		// Config file might be comma-separated (multiple compose files)
		parts := strings.SplitN(configPath, ",", 2)
		primary := strings.TrimSpace(parts[0])
		if !filepath.IsAbs(primary) && workingDir != "" {
			primary = filepath.Join(workingDir, primary)
		}
		data, err := os.ReadFile(primary)
		if err == nil {
			return string(data), nil
		}
	}

	// Fallback: try common filenames in the working directory
	if workingDir != "" {
		candidates := []string{
			"docker-compose.yml",
			"docker-compose.yaml",
			"compose.yml",
			"compose.yaml",
		}
		for _, name := range candidates {
			data, err := os.ReadFile(filepath.Join(workingDir, name))
			if err == nil {
				return string(data), nil
			}
		}
	}

	return "", fmt.Errorf("compose file not found for project %q", project.Name)
}

// ── Compose CLI operations ──────────────────────────────────────────────────

// resolveComposeCLI finds the appropriate compose CLI binary.
// Returns (binary, args-prefix). For Docker: ("docker", ["compose"]).
// For Podman: ("podman", ["compose"]) or ("podman-compose", []).
// For nerdctl: ("nerdctl", ["compose"]).
func resolveComposeCLI(runtimeName string) (string, []string, error) {
	switch runtimeName {
	case "docker":
		// docker compose (v2 plugin) or docker-compose (v1 standalone)
		if path, err := exec.LookPath("docker"); err == nil {
			// Check if "docker compose" subcommand is available
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := exec.CommandContext(ctx, path, "compose", "version").Run(); err == nil {
				return path, []string{"compose"}, nil
			}
		}
		if path, err := exec.LookPath("docker-compose"); err == nil {
			return path, nil, nil
		}
		return "", nil, &containerError{msg: "docker compose not available"}

	case "podman":
		if path, err := exec.LookPath("podman"); err == nil {
			// Check if "podman compose" subcommand is available
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := exec.CommandContext(ctx, path, "compose", "version").Run(); err == nil {
				return path, []string{"compose"}, nil
			}
		}
		if path, err := exec.LookPath("podman-compose"); err == nil {
			return path, nil, nil
		}
		return "", nil, &containerError{msg: "podman compose not available"}

	case "nerdctl", "finch":
		binary := runtimeName
		if path, err := exec.LookPath(binary); err == nil {
			return path, []string{"compose"}, nil
		}
		return "", nil, &containerError{msg: binary + " compose not available"}

	default:
		return "", nil, &containerError{msg: "compose not supported for runtime: " + runtimeName}
	}
}

// runCompose executes a compose command (up, down, restart) for a given project.
func runCompose(runtimeName, projectName, action string, timeout int) error {
	binary, prefix, err := resolveComposeCLI(runtimeName)
	if err != nil {
		return err
	}

	args := append([]string{}, prefix...)
	args = append(args, "-p", projectName)

	switch action {
	case "up":
		args = append(args, "up", "-d")
	case "down":
		if timeout > 0 {
			args = append(args, "down", "-t", strconv.Itoa(timeout))
		} else {
			args = append(args, "down")
		}
	case "restart":
		if timeout > 0 {
			args = append(args, "restart", "-t", strconv.Itoa(timeout))
		} else {
			args = append(args, "restart")
		}
	case "stop":
		if timeout > 0 {
			args = append(args, "stop", "-t", strconv.Itoa(timeout))
		} else {
			args = append(args, "stop")
		}
	case "start":
		args = append(args, "start")
	default:
		return &containerError{msg: "unknown compose action: " + action}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, binary, args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			errMsg := strings.TrimSpace(string(out))
			if errMsg == "" {
				errMsg = strings.TrimSpace(string(exitErr.Stderr))
			}
			return &containerError{msg: fmt.Sprintf("compose %s failed: %s", action, errMsg)}
		}
		return &containerError{msg: fmt.Sprintf("compose %s failed: %v", action, err)}
	}
	return nil
}

// ── Gateway handlers ────────────────────────────────────────────────────────

// handleComposeList lists all compose projects detected from container labels.
func (s *Server) handleComposeList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Fetch all containers (compose projects can have stopped containers too)
	containers, err := adapter.ListContainers(nil)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	projects := listComposeProjects(containers)
	json.NewEncoder(w).Encode(ContainerResponse{ComposeProjects: projects, DurationMs: ms(t0)})
}

// handleComposeFile returns the compose file content for a project.
func (s *Server) handleComposeFile(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.Project == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "project name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Find the project by listing containers and grouping
	containers, err := adapter.ListContainers(nil)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	projects := listComposeProjects(containers)
	var target *ComposeProject
	for i := range projects {
		if projects[i].Name == req.Project {
			target = &projects[i]
			break
		}
	}

	if target == nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "compose project not found: " + req.Project, DurationMs: ms(t0)})
		return
	}

	content, err := readComposeFile(*target)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{ComposeFile: content, DurationMs: ms(t0)})
}

// handleComposeAction runs a compose lifecycle action (up, down, restart, stop, start).
func (s *Server) handleComposeAction(w http.ResponseWriter, req ContainerRequest, action string) {
	t0 := time.Now()

	if req.Project == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "project name is required", DurationMs: ms(t0)})
		return
	}

	// Determine which runtime's compose CLI to use
	runtimeName := req.Runtime
	if runtimeName == "" {
		runtimes := DetectRuntimes()
		for _, rt := range runtimes {
			if rt.Recommended && rt.Available {
				runtimeName = rt.Name
				break
			}
		}
		if runtimeName == "" {
			for _, rt := range runtimes {
				if rt.Available {
					runtimeName = rt.Name
					break
				}
			}
		}
	}

	if runtimeName == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "no container runtime detected", DurationMs: ms(t0)})
		return
	}

	timeout := req.Timeout
	if err := runCompose(runtimeName, req.Project, action, timeout); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}
