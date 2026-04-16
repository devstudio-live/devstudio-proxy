package proxycore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ── Kafka Connect REST proxy ────────────────────────────────────────────────

// connectRequest performs an HTTP request against the Kafka Connect REST API
// configured on the connection. Returns status, body and any transport error.
func (s *Server) connectRequest(conn KafkaConnection, method, path string, body any) (int, []byte, error) {
	base := strings.TrimRight(conn.ConnectURL, "/")
	if base == "" {
		return 0, nil, fmt.Errorf("connectUrl is not configured on the connection")
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal body: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}

	hreq, err := http.NewRequest(method, base+path, reqBody)
	if err != nil {
		return 0, nil, err
	}
	hreq.Header.Set("Accept", "application/json")
	if body != nil {
		hreq.Header.Set("Content-Type", "application/json")
	}

	client := &http.Client{Timeout: queryTimeout}
	hresp, err := client.Do(hreq)
	if err != nil {
		return 0, nil, err
	}
	defer hresp.Body.Close()

	data, err := io.ReadAll(hresp.Body)
	if err != nil {
		return hresp.StatusCode, nil, err
	}
	return hresp.StatusCode, data, nil
}

// writeConnectError formats a Connect REST error body as {error_code, message}.
func writeConnectError(w http.ResponseWriter, status int, body []byte, start time.Time) {
	msg := strings.TrimSpace(string(body))
	var ce struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(body, &ce) == nil && ce.Message != "" {
		msg = fmt.Sprintf("[%d] %s", ce.ErrorCode, ce.Message)
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", status)
	}
	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{Error: msg, DurationMs: dur})
}

// kafkaHandleConnectClusters returns the single configured Connect cluster.
func (s *Server) kafkaHandleConnectClusters(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	url := strings.TrimSpace(req.Connection.ConnectURL)
	clusters := []map[string]any{}
	if url != "" {
		// GET / returns { version, commit, kafka_cluster_id }.
		status, body, err := s.connectRequest(req.Connection, http.MethodGet, "/", nil)
		info := map[string]any{"url": url}
		if err == nil && status < 300 {
			var root map[string]any
			if json.Unmarshal(body, &root) == nil {
				info["version"] = root["version"]
				info["commit"] = root["commit"]
				info["kafkaClusterId"] = root["kafka_cluster_id"]
			}
		}
		clusters = append(clusters, info)
	}
	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"clusters": clusters},
	})
}

// kafkaHandleConnectConnectors lists connectors with status, type and worker
// using the /connectors?expand=info&expand=status endpoint.
func (s *Server) kafkaHandleConnectConnectors(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	status, body, err := s.connectRequest(req.Connection, http.MethodGet, "/connectors?expand=info&expand=status", nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, body, start)
		return
	}

	var raw map[string]struct {
		Info struct {
			Name   string            `json:"name"`
			Config map[string]string `json:"config"`
			Type   string            `json:"type"`
		} `json:"info"`
		Status struct {
			Name      string `json:"name"`
			Type      string `json:"type"`
			Connector struct {
				State    string `json:"state"`
				WorkerID string `json:"worker_id"`
			} `json:"connector"`
			Tasks []struct {
				ID       int    `json:"id"`
				State    string `json:"state"`
				WorkerID string `json:"worker_id"`
				Trace    string `json:"trace,omitempty"`
			} `json:"tasks"`
		} `json:"status"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode connectors: " + err.Error()})
		return
	}

	type taskSummary struct {
		ID       int    `json:"id"`
		State    string `json:"state"`
		WorkerID string `json:"workerId"`
	}
	type connectorSummary struct {
		Name      string            `json:"name"`
		Type      string            `json:"type"`
		State     string            `json:"state"`
		WorkerID  string            `json:"workerId"`
		Class     string            `json:"class"`
		TasksMax  string            `json:"tasksMax"`
		Tasks     []taskSummary     `json:"tasks"`
		FailedCnt int               `json:"failedTaskCount"`
		Config    map[string]string `json:"config,omitempty"`
	}
	list := make([]connectorSummary, 0, len(raw))
	for name, entry := range raw {
		tasks := make([]taskSummary, 0, len(entry.Status.Tasks))
		failed := 0
		for _, t := range entry.Status.Tasks {
			tasks = append(tasks, taskSummary{ID: t.ID, State: t.State, WorkerID: t.WorkerID})
			if strings.EqualFold(t.State, "FAILED") {
				failed++
			}
		}
		cs := connectorSummary{
			Name:      name,
			Type:      strings.ToLower(entry.Info.Type),
			State:     entry.Status.Connector.State,
			WorkerID:  entry.Status.Connector.WorkerID,
			Tasks:     tasks,
			FailedCnt: failed,
		}
		if entry.Info.Config != nil {
			cs.Class = entry.Info.Config["connector.class"]
			cs.TasksMax = entry.Info.Config["tasks.max"]
		}
		list = append(list, cs)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"connectors": list},
	})
}

// kafkaHandleConnectConnectorDetail returns config + status for a single connector.
func (s *Server) kafkaHandleConnectConnectorDetail(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if strings.TrimSpace(req.Connector) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connector name is required"})
		return
	}

	infoStatus, infoBody, err := s.connectRequest(req.Connection, http.MethodGet, "/connectors/"+req.Connector, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if infoStatus >= 300 {
		writeConnectError(w, infoStatus, infoBody, start)
		return
	}
	var info map[string]any
	_ = json.Unmarshal(infoBody, &info)

	statStatus, statBody, err := s.connectRequest(req.Connection, http.MethodGet, "/connectors/"+req.Connector+"/status", nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if statStatus >= 300 {
		writeConnectError(w, statStatus, statBody, start)
		return
	}
	var stat map[string]any
	_ = json.Unmarshal(statBody, &stat)

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"name":   req.Connector,
			"info":   info,
			"status": stat,
		},
	})
}

// kafkaHandleConnectConnectorCreate creates a new connector.
func (s *Server) kafkaHandleConnectConnectorCreate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if strings.TrimSpace(req.Connector) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connector name is required"})
		return
	}
	if req.ConnectorConfig == nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connectorConfig is required"})
		return
	}
	body := map[string]any{
		"name":   req.Connector,
		"config": req.ConnectorConfig,
	}
	status, respBody, err := s.connectRequest(req.Connection, http.MethodPost, "/connectors", body)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, respBody, start)
		return
	}
	var data map[string]any
	_ = json.Unmarshal(respBody, &data)

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

// kafkaHandleConnectConnectorUpdate replaces a connector's config in place.
func (s *Server) kafkaHandleConnectConnectorUpdate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if strings.TrimSpace(req.Connector) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connector name is required"})
		return
	}
	if req.ConnectorConfig == nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connectorConfig is required"})
		return
	}
	status, respBody, err := s.connectRequest(req.Connection, http.MethodPut, "/connectors/"+req.Connector+"/config", req.ConnectorConfig)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, respBody, start)
		return
	}
	var data map[string]any
	_ = json.Unmarshal(respBody, &data)

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

// kafkaHandleConnectConnectorDelete deletes a connector.
func (s *Server) kafkaHandleConnectConnectorDelete(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if strings.TrimSpace(req.Connector) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connector name is required"})
		return
	}
	status, respBody, err := s.connectRequest(req.Connection, http.MethodDelete, "/connectors/"+req.Connector, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, respBody, start)
		return
	}
	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"deleted": req.Connector},
	})
}

// kafkaHandleConnectConnectorPause pauses a connector.
func (s *Server) kafkaHandleConnectConnectorPause(w http.ResponseWriter, req KafkaRequest) {
	s.kafkaConnectStateChange(w, req, http.MethodPut, "/pause", "paused")
}

// kafkaHandleConnectConnectorResume resumes a paused connector.
func (s *Server) kafkaHandleConnectConnectorResume(w http.ResponseWriter, req KafkaRequest) {
	s.kafkaConnectStateChange(w, req, http.MethodPut, "/resume", "resumed")
}

// kafkaHandleConnectConnectorRestart restarts the whole connector, or a single
// task when TaskID > 0.
func (s *Server) kafkaHandleConnectConnectorRestart(w http.ResponseWriter, req KafkaRequest) {
	if req.TaskID > 0 {
		suffix := fmt.Sprintf("/tasks/%d/restart", req.TaskID)
		s.kafkaConnectStateChange(w, req, http.MethodPost, suffix, "task restarted")
		return
	}
	s.kafkaConnectStateChange(w, req, http.MethodPost, "/restart", "restarted")
}

// kafkaConnectStateChange is a helper for pause/resume/restart endpoints.
func (s *Server) kafkaConnectStateChange(w http.ResponseWriter, req KafkaRequest, method, suffix, verb string) {
	start := time.Now()
	if strings.TrimSpace(req.Connector) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connector name is required"})
		return
	}
	status, respBody, err := s.connectRequest(req.Connection, method, "/connectors/"+req.Connector+suffix, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, respBody, start)
		return
	}
	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"connector": req.Connector, "result": verb},
	})
}

// kafkaHandleConnectPlugins lists installed connector plugins.
func (s *Server) kafkaHandleConnectPlugins(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	status, body, err := s.connectRequest(req.Connection, http.MethodGet, "/connector-plugins", nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeConnectError(w, status, body, start)
		return
	}
	var plugins []map[string]any
	if err := json.Unmarshal(body, &plugins); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode plugins: " + err.Error()})
		return
	}
	out := make([]map[string]any, 0, len(plugins))
	for _, p := range plugins {
		out = append(out, map[string]any{
			"class":   p["class"],
			"type":    strings.ToLower(fmt.Sprintf("%v", p["type"])),
			"version": p["version"],
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return fmt.Sprintf("%v", out[i]["class"]) < fmt.Sprintf("%v", out[j]["class"])
	})

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"plugins": out},
	})
}

// ── ACLs ────────────────────────────────────────────────────────────────────

// parseACLResourceType maps a string from the UI to kafka-go's ResourceType.
func parseACLResourceType(s string) (kafka.ResourceType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "any":
		return kafka.ResourceTypeAny, nil
	case "topic":
		return kafka.ResourceTypeTopic, nil
	case "group":
		return kafka.ResourceTypeGroup, nil
	case "cluster":
		return kafka.ResourceTypeCluster, nil
	case "transactionalid", "transactional_id", "transactional-id":
		return kafka.ResourceTypeTransactionalID, nil
	case "delegationtoken", "delegation_token", "delegation-token":
		return kafka.ResourceTypeDelegationToken, nil
	default:
		return 0, fmt.Errorf("unknown resource type: %s", s)
	}
}

func resourceTypeName(rt kafka.ResourceType) string {
	switch rt {
	case kafka.ResourceTypeTopic:
		return "Topic"
	case kafka.ResourceTypeGroup:
		return "Group"
	case kafka.ResourceTypeCluster:
		return "Cluster"
	case kafka.ResourceTypeTransactionalID:
		return "TransactionalId"
	case kafka.ResourceTypeDelegationToken:
		return "DelegationToken"
	case kafka.ResourceTypeAny:
		return "Any"
	default:
		return fmt.Sprintf("Unknown(%d)", int8(rt))
	}
}

func parseACLPatternType(s string) kafka.PatternType {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "literal":
		return kafka.PatternTypeLiteral
	case "prefixed":
		return kafka.PatternTypePrefixed
	case "match":
		return kafka.PatternTypeMatch
	case "any", "":
		return kafka.PatternTypeAny
	default:
		return kafka.PatternTypeAny
	}
}

func patternTypeName(pt kafka.PatternType) string {
	switch pt {
	case kafka.PatternTypeLiteral:
		return "Literal"
	case kafka.PatternTypePrefixed:
		return "Prefixed"
	case kafka.PatternTypeMatch:
		return "Match"
	case kafka.PatternTypeAny:
		return "Any"
	default:
		return fmt.Sprintf("Unknown(%d)", int8(pt))
	}
}

func parseACLOperation(s string) (kafka.ACLOperationType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "any":
		return kafka.ACLOperationTypeAny, nil
	case "all":
		return kafka.ACLOperationTypeAll, nil
	case "read":
		return kafka.ACLOperationTypeRead, nil
	case "write":
		return kafka.ACLOperationTypeWrite, nil
	case "create":
		return kafka.ACLOperationTypeCreate, nil
	case "delete":
		return kafka.ACLOperationTypeDelete, nil
	case "alter":
		return kafka.ACLOperationTypeAlter, nil
	case "describe":
		return kafka.ACLOperationTypeDescribe, nil
	case "clusteraction", "cluster_action":
		return kafka.ACLOperationTypeClusterAction, nil
	case "describeconfigs", "describe_configs":
		return kafka.ACLOperationTypeDescribeConfigs, nil
	case "alterconfigs", "alter_configs":
		return kafka.ACLOperationTypeAlterConfigs, nil
	case "idempotentwrite", "idempotent_write":
		return kafka.ACLOperationTypeIdempotentWrite, nil
	default:
		return 0, fmt.Errorf("unknown operation: %s", s)
	}
}

func parseACLPermission(s string) (kafka.ACLPermissionType, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "any":
		return kafka.ACLPermissionTypeAny, nil
	case "allow":
		return kafka.ACLPermissionTypeAllow, nil
	case "deny":
		return kafka.ACLPermissionTypeDeny, nil
	default:
		return 0, fmt.Errorf("unknown permission: %s", s)
	}
}

// kafkaHandleACLs lists ACL entries. Filter fields (resourceType, resourceName,
// principal, host, operation, permission) narrow results; empty means "any".
func (s *Server) kafkaHandleACLs(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	rt, err := parseACLResourceType(req.ResourceType)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	op, err := parseACLOperation(req.Operation)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	perm, err := parseACLPermission(req.Permission)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	resp, err := client.DescribeACLs(ctx, &kafka.DescribeACLsRequest{
		Addr: client.Addr,
		Filter: kafka.ACLFilter{
			ResourceTypeFilter:        rt,
			ResourceNameFilter:        req.ResourceName,
			ResourcePatternTypeFilter: kafka.PatternTypeAny,
			PrincipalFilter:           req.Principal,
			HostFilter:                req.Host,
			Operation:                 op,
			PermissionType:            perm,
		},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "describe ACLs failed: " + err.Error()})
		return
	}
	if resp.Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: resp.Error.Error()})
		return
	}

	type aclRow struct {
		ResourceType string `json:"resourceType"`
		ResourceName string `json:"resourceName"`
		PatternType  string `json:"patternType"`
		Principal    string `json:"principal"`
		Host         string `json:"host"`
		Operation    string `json:"operation"`
		Permission   string `json:"permission"`
	}
	rows := make([]aclRow, 0)
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			rows = append(rows, aclRow{
				ResourceType: resourceTypeName(r.ResourceType),
				ResourceName: r.ResourceName,
				PatternType:  patternTypeName(r.PatternType),
				Principal:    a.Principal,
				Host:         a.Host,
				Operation:    a.Operation.String(),
				Permission:   a.PermissionType.String(),
			})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ResourceType != rows[j].ResourceType {
			return rows[i].ResourceType < rows[j].ResourceType
		}
		if rows[i].ResourceName != rows[j].ResourceName {
			return rows[i].ResourceName < rows[j].ResourceName
		}
		return rows[i].Principal < rows[j].Principal
	})

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"acls": rows},
	})
}

// kafkaHandleACLCreate creates a single ACL entry.
func (s *Server) kafkaHandleACLCreate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	rt, err := parseACLResourceType(req.ResourceType)
	if err != nil || rt == kafka.ResourceTypeAny {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "resourceType is required (topic/group/cluster/transactionalid)"})
		return
	}
	if strings.TrimSpace(req.ResourceName) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "resourceName is required"})
		return
	}
	if strings.TrimSpace(req.Principal) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "principal is required (e.g. User:alice)"})
		return
	}
	op, err := parseACLOperation(req.Operation)
	if err != nil || op == kafka.ACLOperationTypeAny {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "operation is required"})
		return
	}
	perm, err := parseACLPermission(req.Permission)
	if err != nil || perm == kafka.ACLPermissionTypeAny {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "permission must be allow or deny"})
		return
	}
	host := req.Host
	if host == "" {
		host = "*"
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	resp, err := client.CreateACLs(ctx, &kafka.CreateACLsRequest{
		Addr: client.Addr,
		ACLs: []kafka.ACLEntry{{
			ResourceType:        rt,
			ResourceName:        req.ResourceName,
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           req.Principal,
			Host:                host,
			Operation:           op,
			PermissionType:      perm,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "create ACL failed: " + err.Error()})
		return
	}
	for _, e := range resp.Errors {
		if e != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: e.Error()})
			return
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"created": true},
	})
}

// kafkaHandleACLDelete deletes ACL entries matching the supplied filter.
func (s *Server) kafkaHandleACLDelete(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	rt, err := parseACLResourceType(req.ResourceType)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	op, err := parseACLOperation(req.Operation)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	perm, err := parseACLPermission(req.Permission)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	resp, err := client.DeleteACLs(ctx, &kafka.DeleteACLsRequest{
		Addr: client.Addr,
		Filters: []kafka.DeleteACLsFilter{{
			ResourceTypeFilter:        rt,
			ResourceNameFilter:        req.ResourceName,
			ResourcePatternTypeFilter: kafka.PatternTypeLiteral,
			PrincipalFilter:           req.Principal,
			HostFilter:                req.Host,
			Operation:                 op,
			PermissionType:            perm,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "delete ACL failed: " + err.Error()})
		return
	}

	deleted := 0
	for _, r := range resp.Results {
		if r.Error != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: r.Error.Error()})
			return
		}
		deleted += len(r.MatchingACLs)
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"deleted": deleted},
	})
}

// ── Client Quotas ───────────────────────────────────────────────────────────

// kafkaHandleQuotas lists client quotas configured in the cluster.
func (s *Server) kafkaHandleQuotas(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	resp, err := client.DescribeClientQuotas(ctx, &kafka.DescribeClientQuotasRequest{
		Addr: client.Addr,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "describe quotas failed: " + err.Error()})
		return
	}
	if resp.Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: resp.Error.Error()})
		return
	}

	type quotaRow struct {
		Entities map[string]string  `json:"entities"`
		Values   map[string]float64 `json:"values"`
	}
	rows := make([]quotaRow, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		ents := make(map[string]string, len(e.Entities))
		for _, ent := range e.Entities {
			ents[ent.EntityType] = ent.EntityName
		}
		vals := make(map[string]float64, len(e.Values))
		for _, v := range e.Values {
			vals[v.Key] = v.Value
		}
		rows = append(rows, quotaRow{Entities: ents, Values: vals})
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"quotas": rows},
	})
}
