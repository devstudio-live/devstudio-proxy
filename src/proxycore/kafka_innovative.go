package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// ── Health & Metrics endpoints ─────────────────────────────────────────────

// kafkaHandleClusterHealth returns overall cluster health: broker availability,
// under-replicated partitions, offline partitions, controller presence.
func (s *Server) kafkaHandleClusterHealth(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: client.Addr})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	var underReplicated, offline, totalPartitions int
	topicIssues := make(map[string]int)
	for _, tp := range meta.Topics {
		if tp.Error != nil {
			continue
		}
		for _, p := range tp.Partitions {
			totalPartitions++
			if len(p.Isr) < len(p.Replicas) {
				underReplicated++
				topicIssues[tp.Name]++
			}
			if p.Leader.ID < 0 {
				offline++
				topicIssues[tp.Name]++
			}
		}
	}

	status := "healthy"
	if offline > 0 {
		status = "critical"
	} else if underReplicated > 0 {
		status = "degraded"
	} else if len(meta.Brokers) == 0 {
		status = "critical"
	}

	// Build top problem topics sorted by issue count.
	type issue struct {
		Topic string `json:"topic"`
		Count int    `json:"count"`
	}
	issues := make([]issue, 0, len(topicIssues))
	for tp, c := range topicIssues {
		issues = append(issues, issue{Topic: tp, Count: c})
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].Count > issues[j].Count })
	if len(issues) > 10 {
		issues = issues[:10]
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"status":          status,
			"brokers":         len(meta.Brokers),
			"topics":          len(meta.Topics),
			"partitions":      totalPartitions,
			"underReplicated": underReplicated,
			"offline":         offline,
			"controllerId":    meta.Controller.ID,
			"controllerOk":    meta.Controller.ID >= 0,
			"clusterId":       meta.ClusterID,
			"topicIssues":     issues,
		},
	})
}

// kafkaHandleTopicHealth returns per-topic health: URP, leader skew, ISR coverage.
func (s *Server) kafkaHandleTopicHealth(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic is required"})
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
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   client.Addr,
		Topics: []string{req.Topic},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}
	if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic not found or error"})
		return
	}
	topic := meta.Topics[0]

	var urp, offlineParts int
	leaderCounts := make(map[int]int)
	for _, p := range topic.Partitions {
		if len(p.Isr) < len(p.Replicas) {
			urp++
		}
		if p.Leader.ID < 0 {
			offlineParts++
		} else {
			leaderCounts[p.Leader.ID]++
		}
	}

	// Compute leader skew: max/avg ratio.
	var maxLeaders, totalLeaders int
	for _, c := range leaderCounts {
		if c > maxLeaders {
			maxLeaders = c
		}
		totalLeaders += c
	}
	skew := 0.0
	if len(leaderCounts) > 0 {
		avg := float64(totalLeaders) / float64(len(leaderCounts))
		if avg > 0 {
			skew = float64(maxLeaders) / avg
		}
	}

	status := "healthy"
	if offlineParts > 0 {
		status = "critical"
	} else if urp > 0 {
		status = "degraded"
	} else if skew > 1.5 {
		status = "warn"
	}

	leaderDist := make([]map[string]any, 0, len(leaderCounts))
	for b, c := range leaderCounts {
		leaderDist = append(leaderDist, map[string]any{
			"broker": b,
			"count":  c,
		})
	}
	sort.Slice(leaderDist, func(i, j int) bool {
		return leaderDist[i]["broker"].(int) < leaderDist[j]["broker"].(int)
	})

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":           topic.Name,
			"status":          status,
			"partitions":      len(topic.Partitions),
			"underReplicated": urp,
			"offline":         offlineParts,
			"leaderSkew":      skew,
			"leaderDist":      leaderDist,
		},
	})
}

// kafkaHandleBrokerMetrics returns per-broker metrics: partition counts, leader counts, rack info.
func (s *Server) kafkaHandleBrokerMetrics(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: client.Addr})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	type brokerMetric struct {
		ID         int    `json:"id"`
		Host       string `json:"host"`
		Rack       string `json:"rack"`
		Leaders    int    `json:"leaders"`
		Replicas   int    `json:"replicas"`
		Topics     int    `json:"topics"`
		Controller bool   `json:"controller"`
	}

	byID := make(map[int]*brokerMetric)
	for _, b := range meta.Brokers {
		byID[b.ID] = &brokerMetric{
			ID: b.ID, Host: b.Host, Rack: b.Rack,
			Controller: b.ID == meta.Controller.ID,
		}
	}

	brokerTopics := make(map[int]map[string]bool)
	for _, tp := range meta.Topics {
		if tp.Error != nil {
			continue
		}
		for _, p := range tp.Partitions {
			if p.Leader.ID >= 0 {
				if bm, ok := byID[p.Leader.ID]; ok {
					bm.Leaders++
				}
			}
			for _, r := range p.Replicas {
				if bm, ok := byID[r.ID]; ok {
					bm.Replicas++
				}
				if brokerTopics[r.ID] == nil {
					brokerTopics[r.ID] = make(map[string]bool)
				}
				brokerTopics[r.ID][tp.Name] = true
			}
		}
	}

	result := make([]brokerMetric, 0, len(byID))
	for id, bm := range byID {
		bm.Topics = len(brokerTopics[id])
		result = append(result, *bm)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: result})
}

// kafkaHandleClusterMetrics returns cluster-wide metrics summary (counts + totals).
func (s *Server) kafkaHandleClusterMetrics(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{Addr: client.Addr})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	// Fetch consumer groups count.
	var groupCount int
	if listResp, gErr := client.ListGroups(ctx, &kafka.ListGroupsRequest{Addr: client.Addr}); gErr == nil {
		groupCount = len(listResp.Groups)
	}

	// Compute message totals via offsets.
	offsetReqs := make(map[string][]kafka.OffsetRequest)
	for _, tp := range meta.Topics {
		if tp.Error != nil || tp.Internal {
			continue
		}
		for _, p := range tp.Partitions {
			offsetReqs[tp.Name] = append(offsetReqs[tp.Name],
				kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
		}
	}

	var totalMessages int64
	var nonInternalTopics int
	if len(offsetReqs) > 0 {
		if offResp, oErr := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr: client.Addr, Topics: offsetReqs,
		}); oErr == nil {
			for _, parts := range offResp.Topics {
				for _, po := range parts {
					if po.Error == nil && po.LastOffset > po.FirstOffset {
						totalMessages += po.LastOffset - po.FirstOffset
					}
				}
			}
		}
	}
	for _, tp := range meta.Topics {
		if tp.Error == nil && !tp.Internal {
			nonInternalTopics++
		}
	}

	var totalPartitions, urp int
	for _, tp := range meta.Topics {
		if tp.Error != nil {
			continue
		}
		for _, p := range tp.Partitions {
			totalPartitions++
			if len(p.Isr) < len(p.Replicas) {
				urp++
			}
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"brokers":           len(meta.Brokers),
			"topics":            len(meta.Topics),
			"topicsNonInternal": nonInternalTopics,
			"partitions":        totalPartitions,
			"underReplicated":   urp,
			"groups":            groupCount,
			"messages":          totalMessages,
			"timestamp":         time.Now().UnixMilli(),
		},
	})
}

// ── messages/query ─────────────────────────────────────────────────────────
//
// SQL-like WHERE parser for Kafka messages.
// Supported syntax (case-insensitive keywords, AND only, no OR/parens):
//   SELECT * FROM <topic> [WHERE <expr> [AND <expr> ...]] [LIMIT N]
// Expression forms:
//   <field> = 'value'       (exact match)
//   <field> != 'value'      (not equal)
//   <field> LIKE 'substr'   (substring, case-insensitive)
//   <field> > NUM           (numeric comparison)
//   <field> < NUM
//   <field> >= NUM
//   <field> <= NUM
// Fields: key, value, offset, partition, timestamp, headers.<name>
// Or a JSON path into value: value.foo.bar  (requires value to be JSON)

type queryExpr struct {
	field string
	op    string
	value string
	num   float64
	isNum bool
}

type parsedQuery struct {
	topic string
	where []queryExpr
	limit int
}

func parseKafkaQuery(query string) (*parsedQuery, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("query is empty")
	}
	upper := strings.ToUpper(q)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("query must start with SELECT")
	}
	// Find FROM.
	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return nil, fmt.Errorf("missing FROM clause")
	}
	rest := strings.TrimSpace(q[fromIdx+6:])
	upperRest := strings.ToUpper(rest)

	var topic string
	whereStart := -1
	limitStart := -1
	if i := strings.Index(upperRest, " WHERE "); i >= 0 {
		topic = strings.TrimSpace(rest[:i])
		whereStart = i + 7
	}
	if i := strings.Index(upperRest, " LIMIT "); i >= 0 {
		if whereStart < 0 {
			topic = strings.TrimSpace(rest[:i])
		}
		limitStart = i + 7
	}
	if whereStart < 0 && limitStart < 0 {
		topic = strings.TrimSpace(rest)
	}

	parsed := &parsedQuery{topic: topic, limit: 100}

	// Trim quotes on topic.
	parsed.topic = strings.Trim(parsed.topic, "\"'`")
	if parsed.topic == "" {
		return nil, fmt.Errorf("topic name is required in FROM clause")
	}

	if whereStart >= 0 {
		endIdx := len(rest)
		if limitStart > whereStart {
			endIdx = limitStart - 7
		}
		whereClause := strings.TrimSpace(rest[whereStart:endIdx])
		exprs, err := splitAndParseWhere(whereClause)
		if err != nil {
			return nil, fmt.Errorf("WHERE parse error: %w", err)
		}
		parsed.where = exprs
	}

	if limitStart >= 0 {
		limStr := strings.TrimSpace(rest[limitStart:])
		n, err := strconv.Atoi(limStr)
		if err != nil {
			return nil, fmt.Errorf("LIMIT must be an integer: %v", err)
		}
		parsed.limit = n
	}

	if parsed.limit <= 0 || parsed.limit > 1000 {
		parsed.limit = 100
	}

	return parsed, nil
}

// splitAndParseWhere splits by AND (case-insensitive, not inside quotes)
// and parses each expression.
func splitAndParseWhere(clause string) ([]queryExpr, error) {
	var parts []string
	var buf strings.Builder
	inQuote := byte(0)
	i := 0
	for i < len(clause) {
		c := clause[i]
		if inQuote != 0 {
			buf.WriteByte(c)
			if c == inQuote {
				inQuote = 0
			}
			i++
			continue
		}
		if c == '\'' || c == '"' {
			inQuote = c
			buf.WriteByte(c)
			i++
			continue
		}
		// Check for " AND " boundary.
		if (c == ' ' || c == '\t') && i+4 < len(clause) {
			rem := strings.ToUpper(clause[i : i+5])
			if rem == " AND " {
				parts = append(parts, strings.TrimSpace(buf.String()))
				buf.Reset()
				i += 5
				continue
			}
		}
		buf.WriteByte(c)
		i++
	}
	if buf.Len() > 0 {
		parts = append(parts, strings.TrimSpace(buf.String()))
	}

	exprs := make([]queryExpr, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		e, err := parseOneExpr(p)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
	}
	return exprs, nil
}

func parseOneExpr(s string) (queryExpr, error) {
	// Order matters: check multi-char operators first.
	ops := []string{">=", "<=", "!=", "=", ">", "<"}
	// Handle LIKE separately (case-insensitive, space-delimited).
	upper := strings.ToUpper(s)
	if likeIdx := strings.Index(upper, " LIKE "); likeIdx > 0 {
		field := strings.TrimSpace(s[:likeIdx])
		val := strings.TrimSpace(s[likeIdx+6:])
		val = strings.Trim(val, "'\"")
		return queryExpr{field: field, op: "LIKE", value: val}, nil
	}

	for _, op := range ops {
		if idx := strings.Index(s, op); idx > 0 {
			field := strings.TrimSpace(s[:idx])
			val := strings.TrimSpace(s[idx+len(op):])
			trimmed := strings.Trim(val, "'\"")

			e := queryExpr{field: field, op: op, value: trimmed}
			if op == ">" || op == "<" || op == ">=" || op == "<=" {
				n, err := strconv.ParseFloat(trimmed, 64)
				if err != nil {
					return e, fmt.Errorf("expected number for %s: %q", op, trimmed)
				}
				e.num = n
				e.isNum = true
			}
			return e, nil
		}
	}
	return queryExpr{}, fmt.Errorf("could not parse expression: %q", s)
}

func extractField(msg kafka.Message, field string, valueStr string) (any, bool) {
	switch {
	case field == "key":
		return string(msg.Key), true
	case field == "value":
		return valueStr, true
	case field == "offset":
		return float64(msg.Offset), true
	case field == "partition":
		return float64(msg.Partition), true
	case field == "timestamp":
		return float64(msg.Time.UnixMilli()), true
	case strings.HasPrefix(field, "headers."):
		hname := field[len("headers."):]
		for _, h := range msg.Headers {
			if h.Key == hname {
				return string(h.Value), true
			}
		}
		return "", false
	case strings.HasPrefix(field, "value."):
		// JSON path into value.
		var v any
		if err := json.Unmarshal([]byte(valueStr), &v); err != nil {
			return nil, false
		}
		path := strings.Split(field[len("value."):], ".")
		cur := v
		for _, part := range path {
			m, ok := cur.(map[string]any)
			if !ok {
				return nil, false
			}
			cur, ok = m[part]
			if !ok {
				return nil, false
			}
		}
		return cur, true
	}
	return nil, false
}

func evalExpr(msg kafka.Message, valueStr string, e queryExpr) bool {
	raw, ok := extractField(msg, e.field, valueStr)
	if !ok {
		return false
	}

	// Numeric operations.
	if e.isNum {
		n, ok := toFloat(raw)
		if !ok {
			return false
		}
		switch e.op {
		case ">":
			return n > e.num
		case "<":
			return n < e.num
		case ">=":
			return n >= e.num
		case "<=":
			return n <= e.num
		}
		return false
	}

	var strVal string
	switch v := raw.(type) {
	case string:
		strVal = v
	case float64:
		strVal = strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		strVal = strconv.FormatBool(v)
	default:
		b, _ := json.Marshal(v)
		strVal = string(b)
	}

	switch e.op {
	case "=":
		return strVal == e.value
	case "!=":
		return strVal != e.value
	case "LIKE":
		return strings.Contains(strings.ToLower(strVal), strings.ToLower(e.value))
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case string:
		n, err := strconv.ParseFloat(x, 64)
		if err != nil {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

// kafkaHandleMessagesQuery scans messages and applies a parsed WHERE filter.
func (s *Server) kafkaHandleMessagesQuery(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	parsed, err := parseKafkaQuery(req.Query)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	partitions, err := s.kafkaResolvePartitions(ctx, entry, parsed.topic, req.Partition)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	// Scan budget: up to 10x limit per partition, capped at 5000 total scanned.
	scanBudget := parsed.limit * 10
	if scanBudget > 5000 {
		scanBudget = 5000
	}

	type queryMsg struct {
		Topic     string            `json:"topic"`
		Partition int               `json:"partition"`
		Offset    int64             `json:"offset"`
		Timestamp int64             `json:"timestamp"`
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Headers   map[string]string `json:"headers,omitempty"`
	}

	var matches []queryMsg
	var scanned int

	// Start at latest - scanBudget for each partition to get recent messages.
	// Request BOTH FirstOffset and LastOffset — kafka-go populates only the
	// requested field and leaves the other at -1, which silently corrupts any
	// clamping arithmetic if we ask for only one.
	client := entry.kafkaClient()
	offsetReqs := make(map[string][]kafka.OffsetRequest)
	for _, p := range partitions {
		offsetReqs[parsed.topic] = append(offsetReqs[parsed.topic],
			kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p))
	}
	startOffs := make(map[int]int64)
	if offResp, oErr := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr: client.Addr, Topics: offsetReqs,
	}); oErr == nil {
		budgetPer := scanBudget / len(partitions)
		if budgetPer < 10 {
			budgetPer = 10
		}
		for _, po := range offResp.Topics[parsed.topic] {
			first := po.FirstOffset
			if first < 0 {
				first = 0
			}
			off := po.LastOffset - int64(budgetPer)
			if off < first {
				off = first
			}
			startOffs[po.Partition] = off
		}
	}

	for _, p := range partitions {
		if len(matches) >= parsed.limit || scanned >= scanBudget {
			break
		}
		startOff, ok := startOffs[p]
		if !ok {
			startOff = -2
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: entry.brokers, Topic: parsed.topic, Partition: p,
			MinBytes: 1, MaxBytes: 1e6, Dialer: entry.dialer,
		})
		reader.SetOffset(startOff)

		// Cold-start a reader can take longer than a single-message read budget;
		// give the first read a generous timeout, then shorter timeouts for
		// subsequent reads since the reader is warm at that point.
		firstRead := true
		for scanned < scanBudget && len(matches) < parsed.limit {
			readTO := 2 * time.Second
			if firstRead {
				readTO = 10 * time.Second
			}
			readCtx, readCancel := context.WithTimeout(ctx, readTO)
			m, err := reader.ReadMessage(readCtx)
			readCancel()
			firstRead = false
			if err != nil {
				break
			}
			scanned++
			valueStr := string(m.Value)

			matched := true
			for _, e := range parsed.where {
				if !evalExpr(m, valueStr, e) {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}

			hdrs := make(map[string]string)
			for _, h := range m.Headers {
				hdrs[h.Key] = string(h.Value)
			}
			if len(valueStr) > maxMessageValueLen {
				valueStr = valueStr[:maxMessageValueLen]
			}
			matches = append(matches, queryMsg{
				Topic: m.Topic, Partition: m.Partition, Offset: m.Offset,
				Timestamp: m.Time.UnixMilli(),
				Key:       string(m.Key), Value: valueStr, Headers: hdrs,
			})
		}
		reader.Close()
	}

	sort.Slice(matches, func(i, j int) bool { return matches[i].Timestamp > matches[j].Timestamp })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":    parsed.topic,
			"scanned":  scanned,
			"matched":  len(matches),
			"limit":    parsed.limit,
			"messages": matches,
		},
	})
}

// ── messages/trace ─────────────────────────────────────────────────────────

// kafkaHandleMessagesTrace searches for messages with a matching key (or header)
// across multiple topics to reconstruct a cross-topic correlation timeline.
func (s *Server) kafkaHandleMessagesTrace(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if len(req.TraceTopics) == 0 {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "traceTopics is required (list of topics)"})
		return
	}
	if req.TraceKey == "" && req.TraceValue == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "traceKey or traceValue is required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	traceKey := req.TraceKey
	traceVal := strings.ToLower(req.TraceValue)

	scanBudget := req.Limit
	if scanBudget <= 0 {
		scanBudget = 2000
	}
	if scanBudget > 10000 {
		scanBudget = 10000
	}

	type traceHit struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Timestamp int64  `json:"timestamp"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	hits := make([]traceHit, 0)
	scanned := 0
	perTopicBudget := scanBudget / len(req.TraceTopics)
	if perTopicBudget < 50 {
		perTopicBudget = 50
	}

	client := entry.kafkaClient()

	for _, topic := range req.TraceTopics {
		// Per-topic scan budget.
		topicScanned := 0
		partitions, err := s.kafkaResolvePartitions(ctx, entry, topic, -1)
		if err != nil {
			continue
		}

		// Start from latest - perTopicBudget.
		offsetReqs := make(map[string][]kafka.OffsetRequest)
		for _, p := range partitions {
			offsetReqs[topic] = append(offsetReqs[topic],
				kafka.FirstOffsetOf(p), kafka.LastOffsetOf(p))
		}
		startOffs := make(map[int]int64)
		if offResp, oErr := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr: client.Addr, Topics: offsetReqs,
		}); oErr == nil {
			budgetPerPart := perTopicBudget / len(partitions)
			if budgetPerPart < 10 {
				budgetPerPart = 10
			}
			for _, po := range offResp.Topics[topic] {
				first := po.FirstOffset
				if first < 0 {
					first = 0
				}
				off := po.LastOffset - int64(budgetPerPart)
				if off < first {
					off = first
				}
				startOffs[po.Partition] = off
			}
		}

		for _, p := range partitions {
			if topicScanned >= perTopicBudget {
				break
			}
			startOff, ok := startOffs[p]
			if !ok {
				startOff = -2
			}

			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers: entry.brokers, Topic: topic, Partition: p,
				MinBytes: 1, MaxBytes: 1e6, Dialer: entry.dialer,
			})
			reader.SetOffset(startOff)

			for topicScanned < perTopicBudget {
				readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
				m, err := reader.ReadMessage(readCtx)
				readCancel()
				if err != nil {
					break
				}
				topicScanned++
				scanned++

				keyStr := string(m.Key)
				valStr := string(m.Value)
				matched := false
				if traceKey != "" && keyStr == traceKey {
					matched = true
				}
				if !matched && traceVal != "" && strings.Contains(strings.ToLower(valStr), traceVal) {
					matched = true
				}
				if !matched {
					// Check headers for trace value.
					if traceVal != "" {
						for _, h := range m.Headers {
							if strings.Contains(strings.ToLower(string(h.Value)), traceVal) {
								matched = true
								break
							}
						}
					}
				}
				if !matched {
					continue
				}

				if len(valStr) > maxMessageValueLen {
					valStr = valStr[:maxMessageValueLen]
				}
				hits = append(hits, traceHit{
					Topic: topic, Partition: m.Partition, Offset: m.Offset,
					Timestamp: m.Time.UnixMilli(),
					Key:       keyStr, Value: valStr,
				})
			}
			reader.Close()
		}
	}

	// Sort by timestamp ascending (timeline order).
	sort.Slice(hits, func(i, j int) bool { return hits[i].Timestamp < hits[j].Timestamp })

	// Build per-topic counts.
	byTopic := make(map[string]int)
	for _, h := range hits {
		byTopic[h.Topic]++
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"scanned":  scanned,
			"matched":  len(hits),
			"hits":     hits,
			"byTopic":  byTopic,
			"traceKey": traceKey,
		},
	})
}

// ── messages/diff ──────────────────────────────────────────────────────────

// kafkaHandleMessagesDiff compares two messages (by topic+partition+offset pairs)
// and returns a structured diff of keys, values, and headers.
func (s *Server) kafkaHandleMessagesDiff(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" || req.DiffTopic2 == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic and diffTopic2 are required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	readOne := func(topic string, partition int, offset int64) (*kafka.Message, error) {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers: entry.brokers, Topic: topic, Partition: partition,
			MinBytes: 1, MaxBytes: 10e6, Dialer: entry.dialer,
		})
		defer r.Close()
		r.SetOffset(offset)
		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		defer readCancel()
		m, err := r.ReadMessage(readCtx)
		if err != nil {
			return nil, err
		}
		return &m, nil
	}

	m1, err := readOne(req.Topic, req.Partition, req.Offset)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "read msg1 failed: " + err.Error()})
		return
	}
	m2, err := readOne(req.DiffTopic2, req.DiffPartition2, req.DiffOffset2)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "read msg2 failed: " + err.Error()})
		return
	}

	diff := diffMessages(*m1, *m2)

	msgSummary := func(m kafka.Message) map[string]any {
		hdrs := make(map[string]string)
		for _, h := range m.Headers {
			hdrs[h.Key] = string(h.Value)
		}
		return map[string]any{
			"topic":     m.Topic,
			"partition": m.Partition,
			"offset":    m.Offset,
			"timestamp": m.Time.UnixMilli(),
			"key":       string(m.Key),
			"value":     string(m.Value),
			"headers":   hdrs,
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"msg1": msgSummary(*m1),
			"msg2": msgSummary(*m2),
			"diff": diff,
		},
	})
}

type diffEntry struct {
	Path   string `json:"path"`
	Status string `json:"status"` // "added" | "removed" | "changed" | "same"
	Left   any    `json:"left,omitempty"`
	Right  any    `json:"right,omitempty"`
}

func diffMessages(m1, m2 kafka.Message) map[string]any {
	var entries []diffEntry

	// Key diff.
	k1, k2 := string(m1.Key), string(m2.Key)
	if k1 == k2 {
		entries = append(entries, diffEntry{Path: "key", Status: "same", Left: k1, Right: k2})
	} else {
		entries = append(entries, diffEntry{Path: "key", Status: "changed", Left: k1, Right: k2})
	}

	// Headers diff.
	h1 := make(map[string]string)
	for _, h := range m1.Headers {
		h1[h.Key] = string(h.Value)
	}
	h2 := make(map[string]string)
	for _, h := range m2.Headers {
		h2[h.Key] = string(h.Value)
	}
	allHdrs := make(map[string]bool)
	for k := range h1 {
		allHdrs[k] = true
	}
	for k := range h2 {
		allHdrs[k] = true
	}
	hkeys := make([]string, 0, len(allHdrs))
	for k := range allHdrs {
		hkeys = append(hkeys, k)
	}
	sort.Strings(hkeys)
	for _, k := range hkeys {
		v1, ok1 := h1[k]
		v2, ok2 := h2[k]
		path := "headers." + k
		switch {
		case ok1 && !ok2:
			entries = append(entries, diffEntry{Path: path, Status: "removed", Left: v1})
		case !ok1 && ok2:
			entries = append(entries, diffEntry{Path: path, Status: "added", Right: v2})
		case v1 != v2:
			entries = append(entries, diffEntry{Path: path, Status: "changed", Left: v1, Right: v2})
		default:
			entries = append(entries, diffEntry{Path: path, Status: "same", Left: v1, Right: v2})
		}
	}

	// Value diff — try JSON parse, else string compare.
	var v1, v2 any
	jsonOK := true
	if err := json.Unmarshal(m1.Value, &v1); err != nil {
		jsonOK = false
	}
	if err := json.Unmarshal(m2.Value, &v2); err != nil {
		jsonOK = false
	}

	var valueEntries []diffEntry
	if jsonOK {
		valueEntries = diffJSON("value", v1, v2)
	} else {
		s1, s2 := string(m1.Value), string(m2.Value)
		if s1 == s2 {
			valueEntries = []diffEntry{{Path: "value", Status: "same", Left: s1, Right: s2}}
		} else {
			valueEntries = []diffEntry{{Path: "value", Status: "changed", Left: s1, Right: s2}}
		}
	}
	entries = append(entries, valueEntries...)

	// Stats.
	var added, removed, changed, same int
	for _, e := range entries {
		switch e.Status {
		case "added":
			added++
		case "removed":
			removed++
		case "changed":
			changed++
		case "same":
			same++
		}
	}

	return map[string]any{
		"entries": entries,
		"stats": map[string]int{
			"added":   added,
			"removed": removed,
			"changed": changed,
			"same":    same,
			"total":   len(entries),
		},
		"jsonParsed": jsonOK,
	}
}

// diffJSON recursively compares two decoded JSON values and returns diff entries.
func diffJSON(path string, a, b any) []diffEntry {
	// Same type + scalar equality.
	if isScalar(a) && isScalar(b) {
		if scalarEqual(a, b) {
			return []diffEntry{{Path: path, Status: "same", Left: a, Right: b}}
		}
		return []diffEntry{{Path: path, Status: "changed", Left: a, Right: b}}
	}

	aMap, aOK := a.(map[string]any)
	bMap, bOK := b.(map[string]any)
	if aOK && bOK {
		keys := make(map[string]bool)
		for k := range aMap {
			keys[k] = true
		}
		for k := range bMap {
			keys[k] = true
		}
		sortedKeys := make([]string, 0, len(keys))
		for k := range keys {
			sortedKeys = append(sortedKeys, k)
		}
		sort.Strings(sortedKeys)

		var out []diffEntry
		for _, k := range sortedKeys {
			sub := path + "." + k
			av, aHas := aMap[k]
			bv, bHas := bMap[k]
			switch {
			case aHas && !bHas:
				out = append(out, diffEntry{Path: sub, Status: "removed", Left: av})
			case !aHas && bHas:
				out = append(out, diffEntry{Path: sub, Status: "added", Right: bv})
			default:
				out = append(out, diffJSON(sub, av, bv)...)
			}
		}
		return out
	}

	aArr, aAOK := a.([]any)
	bArr, bAOK := b.([]any)
	if aAOK && bAOK {
		n := len(aArr)
		if len(bArr) > n {
			n = len(bArr)
		}
		var out []diffEntry
		for i := 0; i < n; i++ {
			sub := fmt.Sprintf("%s[%d]", path, i)
			switch {
			case i >= len(aArr):
				out = append(out, diffEntry{Path: sub, Status: "added", Right: bArr[i]})
			case i >= len(bArr):
				out = append(out, diffEntry{Path: sub, Status: "removed", Left: aArr[i]})
			default:
				out = append(out, diffJSON(sub, aArr[i], bArr[i])...)
			}
		}
		return out
	}

	// Type mismatch.
	return []diffEntry{{Path: path, Status: "changed", Left: a, Right: b}}
}

func isScalar(v any) bool {
	switch v.(type) {
	case string, float64, float32, int, int64, bool, nil:
		return true
	}
	return false
}

func scalarEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Use JSON encoding for consistent comparison.
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}
