package proxycore

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// handleKafkaGateway is the entry-point for all Kafka gateway requests.
func (s *Server) handleKafkaGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(KafkaResponse{Error: "only POST is accepted"})
		return
	}

	var req KafkaRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(KafkaResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "test":
		s.kafkaHandleTest(w, req)
	case "brokers":
		s.kafkaHandleBrokers(w, req)
	case "broker/config":
		s.kafkaHandleBrokerConfig(w, req)
	case "topics":
		s.kafkaHandleTopics(w, req)
	case "topic/detail":
		s.kafkaHandleTopicDetail(w, req)
	case "topic/create":
		s.kafkaHandleTopicCreate(w, req)
	case "topic/delete":
		s.kafkaHandleTopicDelete(w, req)
	case "topic/config/update":
		s.kafkaHandleTopicConfigUpdate(w, req)
	case "topic/partitions/add":
		s.kafkaHandleTopicPartitionsAdd(w, req)
	case "topic/purge":
		s.kafkaHandleTopicPurge(w, req)
	case "topic/clone":
		s.kafkaHandleTopicClone(w, req)
	case "schemas":
		s.kafkaHandleSchemas(w, req)
	case "schema/detail":
		s.kafkaHandleSchemaDetail(w, req)
	case "schema/version":
		s.kafkaHandleSchemaVersion(w, req)
	case "schema/create":
		s.kafkaHandleSchemaCreate(w, req)
	case "schema/delete":
		s.kafkaHandleSchemaDelete(w, req)
	case "schema/compatibility":
		s.kafkaHandleSchemaCompatibility(w, req)
	case "schema/config":
		s.kafkaHandleSchemaConfig(w, req)
	case "messages":
		s.kafkaHandleMessages(w, req)
	case "messages/produce":
		s.kafkaHandleMessagesProduce(w, req)
	case "messages/full":
		s.kafkaHandleMessagesFull(w, req)
	case "messages/tail":
		s.kafkaHandleMessagesTail(w, r, req)
	case "messages/export":
		s.kafkaHandleMessagesExport(w, req)
	case "groups":
		s.kafkaHandleGroups(w, req)
	case "group/detail":
		s.kafkaHandleGroupDetail(w, req)
	case "group/reset-offsets":
		s.kafkaHandleGroupResetOffsets(w, req)
	case "group/delete":
		s.kafkaHandleGroupDelete(w, req)
	case "connect/clusters":
		s.kafkaHandleConnectClusters(w, req)
	case "connect/connectors":
		s.kafkaHandleConnectConnectors(w, req)
	case "connect/connector/detail":
		s.kafkaHandleConnectConnectorDetail(w, req)
	case "connect/connector/create":
		s.kafkaHandleConnectConnectorCreate(w, req)
	case "connect/connector/update":
		s.kafkaHandleConnectConnectorUpdate(w, req)
	case "connect/connector/delete":
		s.kafkaHandleConnectConnectorDelete(w, req)
	case "connect/connector/pause":
		s.kafkaHandleConnectConnectorPause(w, req)
	case "connect/connector/resume":
		s.kafkaHandleConnectConnectorResume(w, req)
	case "connect/connector/restart":
		s.kafkaHandleConnectConnectorRestart(w, req)
	case "connect/plugins":
		s.kafkaHandleConnectPlugins(w, req)
	case "acls":
		s.kafkaHandleACLs(w, req)
	case "acl/create":
		s.kafkaHandleACLCreate(w, req)
	case "acl/delete":
		s.kafkaHandleACLDelete(w, req)
	case "quotas":
		s.kafkaHandleQuotas(w, req)
	case "cluster/health":
		s.kafkaHandleClusterHealth(w, req)
	case "topic/health":
		s.kafkaHandleTopicHealth(w, req)
	case "broker/metrics":
		s.kafkaHandleBrokerMetrics(w, req)
	case "cluster/metrics":
		s.kafkaHandleClusterMetrics(w, req)
	case "messages/query":
		s.kafkaHandleMessagesQuery(w, req)
	case "messages/trace":
		s.kafkaHandleMessagesTrace(w, req)
	case "messages/diff":
		s.kafkaHandleMessagesDiff(w, req)
	default:
		json.NewEncoder(w).Encode(KafkaResponse{Error: "unknown endpoint: " + path})
	}
}

// kafkaHandleTest dials the first broker and verifies connectivity.
func (s *Server) kafkaHandleTest(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	conn, err := entry.kafkaDial()
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "connection failed: " + err.Error()})
		return
	}
	defer conn.Close()

	// Fetch metadata to verify the cluster is responsive and count brokers.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: client.Addr,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"brokers":    len(meta.Brokers),
			"controller": meta.Controller.ID,
			"clusterID":  meta.ClusterID,
		},
	})
}

// kafkaHandleBrokers lists all brokers with metadata.
func (s *Server) kafkaHandleBrokers(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: client.Addr,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	type brokerInfo struct {
		ID           int    `json:"id"`
		Host         string `json:"host"`
		Port         int    `json:"port"`
		Rack         string `json:"rack"`
		IsController bool   `json:"isController"`
	}

	brokers := make([]brokerInfo, len(meta.Brokers))
	for i, b := range meta.Brokers {
		brokers[i] = brokerInfo{
			ID:           b.ID,
			Host:         b.Host,
			Port:         b.Port,
			Rack:         b.Rack,
			IsController: b.ID == meta.Controller.ID,
		}
	}

	sort.Slice(brokers, func(i, j int) bool { return brokers[i].ID < brokers[j].ID })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       brokers,
	})
}

// kafkaHandleBrokerConfig describes broker configuration via DescribeConfigs.
func (s *Server) kafkaHandleBrokerConfig(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	// Default to broker ID 0 from the request's Topic field (overloaded as broker ID string).
	brokerID := "0"
	if req.Topic != "" {
		brokerID = req.Topic
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	resp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Addr: client.Addr,
		Resources: []kafka.DescribeConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeBroker,
				ResourceName: brokerID,
			},
		},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "describe config failed: " + err.Error()})
		return
	}

	type configEntry struct {
		Name        string `json:"name"`
		Value       string `json:"value"`
		ReadOnly    bool   `json:"readOnly"`
		IsSensitive bool   `json:"isSensitive"`
		IsDefault   bool   `json:"isDefault"`
	}

	var configs []configEntry
	for _, res := range resp.Resources {
		if res.Error != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "broker config error: " + res.Error.Error()})
			return
		}
		for _, e := range res.ConfigEntries {
			configs = append(configs, configEntry{
				Name:        e.ConfigName,
				Value:       e.ConfigValue,
				ReadOnly:    e.ReadOnly,
				IsSensitive: e.IsSensitive,
				IsDefault:   e.IsDefault,
			})
		}
	}

	sort.Slice(configs, func(i, j int) bool { return configs[i].Name < configs[j].Name })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       configs,
	})
}

// kafkaHandleTopics lists all topics with partition count, replica count, message count, and internal flag.
func (s *Server) kafkaHandleTopics(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr: client.Addr,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	// Gather topic names for offset queries.
	offsetReqs := make(map[string][]kafka.OffsetRequest)
	for _, t := range meta.Topics {
		if t.Error != nil {
			continue
		}
		for _, p := range t.Partitions {
			offsetReqs[t.Name] = append(offsetReqs[t.Name],
				kafka.FirstOffsetOf(p.ID),
				kafka.LastOffsetOf(p.ID),
			)
		}
	}

	// Fetch offsets to estimate message counts.
	var offsetsResp *kafka.ListOffsetsResponse
	if len(offsetReqs) > 0 {
		offsetsResp, err = client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr:   client.Addr,
			Topics: offsetReqs,
		})
		if err != nil {
			// Non-fatal: we can still return topics without message counts.
			offsetsResp = nil
		}
	}

	type topicInfo struct {
		Name       string `json:"name"`
		Partitions int    `json:"partitions"`
		Replicas   int    `json:"replicas"`
		Messages   int64  `json:"messages"`
		Internal   bool   `json:"internal"`
	}

	search := strings.ToLower(req.Search)
	var topics []topicInfo
	for _, t := range meta.Topics {
		if t.Error != nil {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(t.Name), search) {
			continue
		}

		replicas := 0
		if len(t.Partitions) > 0 {
			replicas = len(t.Partitions[0].Replicas)
		}

		var messageCount int64
		if offsetsResp != nil {
			if partOffsets, ok := offsetsResp.Topics[t.Name]; ok {
				for _, po := range partOffsets {
					if po.Error == nil && po.LastOffset > po.FirstOffset {
						messageCount += po.LastOffset - po.FirstOffset
					}
				}
			}
		}

		topics = append(topics, topicInfo{
			Name:       t.Name,
			Partitions: len(t.Partitions),
			Replicas:   replicas,
			Messages:   messageCount,
			Internal:   t.Internal,
		})
	}

	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       topics,
	})
}

// kafkaHandleTopicDetail returns partition layout + topic configuration.
func (s *Server) kafkaHandleTopicDetail(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
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

	// Fetch metadata for this specific topic.
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   client.Addr,
		Topics: []string{req.Topic},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata fetch failed: " + err.Error()})
		return
	}

	if len(meta.Topics) == 0 {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic not found: " + req.Topic})
		return
	}

	topic := meta.Topics[0]
	if topic.Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic error: " + topic.Error.Error()})
		return
	}

	// Fetch offsets for the topic's partitions.
	offsetReqs := make([]kafka.OffsetRequest, 0, len(topic.Partitions)*2)
	for _, p := range topic.Partitions {
		offsetReqs = append(offsetReqs,
			kafka.FirstOffsetOf(p.ID),
			kafka.LastOffsetOf(p.ID),
		)
	}

	var offsetsResp *kafka.ListOffsetsResponse
	if len(offsetReqs) > 0 {
		offsetsResp, err = client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr:   client.Addr,
			Topics: map[string][]kafka.OffsetRequest{req.Topic: offsetReqs},
		})
		if err != nil {
			offsetsResp = nil
		}
	}

	type partitionInfo struct {
		ID       int    `json:"id"`
		Leader   int    `json:"leader"`
		Replicas []int  `json:"replicas"`
		ISR      []int  `json:"isr"`
		OffsetLo int64  `json:"offsetLo"`
		OffsetHi int64  `json:"offsetHi"`
	}

	partitions := make([]partitionInfo, len(topic.Partitions))
	for i, p := range topic.Partitions {
		reps := make([]int, len(p.Replicas))
		for j, r := range p.Replicas {
			reps[j] = r.ID
		}
		isr := make([]int, len(p.Isr))
		for j, r := range p.Isr {
			isr[j] = r.ID
		}

		var lo, hi int64
		if offsetsResp != nil {
			if partOffsets, ok := offsetsResp.Topics[req.Topic]; ok {
				for _, po := range partOffsets {
					if po.Partition == p.ID && po.Error == nil {
						if po.FirstOffset >= 0 {
							lo = po.FirstOffset
						}
						if po.LastOffset >= 0 {
							hi = po.LastOffset
						}
					}
				}
			}
		}

		partitions[i] = partitionInfo{
			ID:       p.ID,
			Leader:   p.Leader.ID,
			Replicas: reps,
			ISR:      isr,
			OffsetLo: lo,
			OffsetHi: hi,
		}
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].ID < partitions[j].ID })

	// Fetch topic configuration.
	configResp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Addr: client.Addr,
		Resources: []kafka.DescribeConfigRequestResource{
			{
				ResourceType: kafka.ResourceTypeTopic,
				ResourceName: req.Topic,
			},
		},
	})

	type configEntry struct {
		Name        string `json:"name"`
		Value       string `json:"value"`
		ReadOnly    bool   `json:"readOnly"`
		IsSensitive bool   `json:"isSensitive"`
		IsDefault   bool   `json:"isDefault"`
	}

	var configs []configEntry
	if err == nil {
		for _, res := range configResp.Resources {
			if res.Error != nil {
				continue
			}
			for _, e := range res.ConfigEntries {
				configs = append(configs, configEntry{
					Name:        e.ConfigName,
					Value:       e.ConfigValue,
					ReadOnly:    e.ReadOnly,
					IsSensitive: e.IsSensitive,
					IsDefault:   e.IsDefault,
				})
			}
		}
		sort.Slice(configs, func(i, j int) bool { return configs[i].Name < configs[j].Name })
	}

	// Compute replicas from first partition.
	replicas := 0
	if len(topic.Partitions) > 0 {
		replicas = len(topic.Partitions[0].Replicas)
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"name":       topic.Name,
			"internal":   topic.Internal,
			"partitions": partitions,
			"replicas":   replicas,
			"config":     configs,
		},
	})
}

// ── Message endpoints ──────────────────────────────────────────────────────

const maxMessageValueLen = 10 * 1024 // 10 KB truncation for /messages

// kafkaHandleMessages consumes messages from a topic with offset/timestamp seek.
func (s *Server) kafkaHandleMessages(w http.ResponseWriter, req KafkaRequest) {
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

	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	// Resolve which partitions to read.
	partitions, err := s.kafkaResolvePartitions(ctx, entry, req.Topic, req.Partition)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	// If timestamp seek, resolve to offsets first.
	startOffsets := make(map[int]int64)
	if req.Timestamp > 0 {
		resolved, err := s.kafkaTimestampToOffsets(ctx, entry, req.Topic, partitions, req.Timestamp)
		if err != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "timestamp resolve failed: " + err.Error()})
			return
		}
		startOffsets = resolved
	} else if req.Offset >= 0 {
		for _, p := range partitions {
			startOffsets[p] = req.Offset
		}
	} else {
		// Default: latest minus limit (show most recent messages)
		client := entry.kafkaClient()
		offsetReqs := make(map[string][]kafka.OffsetRequest)
		for _, p := range partitions {
			offsetReqs[req.Topic] = append(offsetReqs[req.Topic], kafka.LastOffsetOf(p))
		}
		offResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
			Addr:   client.Addr,
			Topics: offsetReqs,
		})
		if err == nil {
			for _, po := range offResp.Topics[req.Topic] {
				off := po.LastOffset - int64(limit)
				if off < po.FirstOffset {
					off = po.FirstOffset
				}
				startOffsets[po.Partition] = off
			}
		} else {
			for _, p := range partitions {
				startOffsets[p] = -2 // earliest
			}
		}
	}

	search := strings.ToLower(req.Search)

	type messageOut struct {
		Topic     string            `json:"topic"`
		Partition int               `json:"partition"`
		Offset    int64             `json:"offset"`
		Timestamp int64             `json:"timestamp"`
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		HasMore   bool              `json:"hasMore,omitempty"`
		Headers   map[string]string `json:"headers,omitempty"`
	}

	var messages []messageOut
	for _, p := range partitions {
		if len(messages) >= limit {
			break
		}
		startOff, ok := startOffsets[p]
		if !ok {
			startOff = -2
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   entry.brokers,
			Topic:     req.Topic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  1e6,
			Dialer:    entry.dialer,
		})
		reader.SetOffset(startOff)
		defer reader.Close()

		perPartLimit := limit - len(messages)
		for i := 0; i < perPartLimit; i++ {
			readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
			m, err := reader.ReadMessage(readCtx)
			readCancel()
			if err != nil {
				break
			}

			key := string(m.Key)
			value := string(m.Value)

			if search != "" {
				if !strings.Contains(strings.ToLower(key), search) &&
					!strings.Contains(strings.ToLower(value), search) {
					i-- // don't count filtered messages against limit
					continue
				}
			}

			hasMore := false
			if len(value) > maxMessageValueLen {
				value = value[:maxMessageValueLen]
				hasMore = true
			}

			hdrs := make(map[string]string)
			for _, h := range m.Headers {
				hdrs[h.Key] = string(h.Value)
			}

			messages = append(messages, messageOut{
				Topic:     m.Topic,
				Partition: m.Partition,
				Offset:    m.Offset,
				Timestamp: m.Time.UnixMilli(),
				Key:       key,
				Value:     value,
				HasMore:   hasMore,
				Headers:   hdrs,
			})
		}
	}

	// Sort by timestamp descending (most recent first).
	sort.Slice(messages, func(i, j int) bool { return messages[i].Timestamp > messages[j].Timestamp })
	if len(messages) > limit {
		messages = messages[:limit]
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       messages,
	})
}

// kafkaHandleMessagesProduce produces a message to a topic.
func (s *Server) kafkaHandleMessagesProduce(w http.ResponseWriter, req KafkaRequest) {
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

	msg := kafka.Message{
		Topic: req.Topic,
		Key:   []byte(req.Key),
		Value: []byte(req.Value),
	}

	for k, v := range req.Headers {
		msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(v)})
	}

	writer := &kafka.Writer{
		Addr:      kafka.TCP(entry.brokers...),
		Topic:     req.Topic,
		Transport: entry.transport,
	}
	if req.Partition >= 0 {
		writer.Balancer = &kafka.RoundRobin{} // partition selection is via the message
		msg.Partition = req.Partition
	}
	defer writer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	if err := writer.WriteMessages(ctx, msg); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "produce failed: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":     req.Topic,
			"partition": req.Partition,
		},
	})
}

// kafkaHandleMessagesFull returns the full untruncated value of a single message.
func (s *Server) kafkaHandleMessagesFull(w http.ResponseWriter, req KafkaRequest) {
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

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   entry.brokers,
		Topic:     req.Topic,
		Partition: req.Partition,
		MinBytes:  1,
		MaxBytes:  10e6,
		Dialer:    entry.dialer,
	})
	reader.SetOffset(req.Offset)
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	m, err := reader.ReadMessage(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "read failed: " + err.Error()})
		return
	}

	hdrs := make(map[string]string)
	for _, h := range m.Headers {
		hdrs[h.Key] = string(h.Value)
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":     m.Topic,
			"partition": m.Partition,
			"offset":    m.Offset,
			"timestamp": m.Time.UnixMilli(),
			"key":       string(m.Key),
			"value":     string(m.Value),
			"headers":   hdrs,
		},
	})
}

// kafkaHandleMessagesTail streams new messages via SSE.
func (s *Server) kafkaHandleMessagesTail(w http.ResponseWriter, r *http.Request, req KafkaRequest) {
	if req.Topic == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic is required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   entry.brokers,
		Topic:     req.Topic,
		Partition: req.Partition,
		MinBytes:  1,
		MaxBytes:  1e6,
		Dialer:    entry.dialer,
	})
	reader.SetOffset(-1) // latest
	defer reader.Close()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
		m, err := reader.ReadMessage(readCtx)
		readCancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue // timeout — loop back to check ctx
		}

		hdrs := make(map[string]string)
		for _, h := range m.Headers {
			hdrs[h.Key] = string(h.Value)
		}

		value := string(m.Value)
		hasMore := false
		if len(value) > maxMessageValueLen {
			value = value[:maxMessageValueLen]
			hasMore = true
		}

		evt := map[string]any{
			"topic":     m.Topic,
			"partition": m.Partition,
			"offset":    m.Offset,
			"timestamp": m.Time.UnixMilli(),
			"key":       string(m.Key),
			"value":     value,
			"hasMore":   hasMore,
			"headers":   hdrs,
		}
		b, _ := json.Marshal(evt)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
}

// kafkaHandleMessagesExport exports messages as JSON or CSV.
func (s *Server) kafkaHandleMessagesExport(w http.ResponseWriter, req KafkaRequest) {
	if req.Topic == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic is required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = 500
	}
	if limit > 5000 {
		limit = 5000
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	partitions, err := s.kafkaResolvePartitions(ctx, entry, req.Topic, req.Partition)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	type exportMsg struct {
		Partition int               `json:"partition"`
		Offset    int64             `json:"offset"`
		Timestamp int64             `json:"timestamp"`
		Key       string            `json:"key"`
		Value     string            `json:"value"`
		Headers   map[string]string `json:"headers,omitempty"`
	}

	var messages []exportMsg
	for _, p := range partitions {
		if len(messages) >= limit {
			break
		}
		startOff := req.Offset
		if startOff < 0 {
			startOff = -2 // earliest
		}

		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   entry.brokers,
			Topic:     req.Topic,
			Partition: p,
			MinBytes:  1,
			MaxBytes:  1e6,
			Dialer:    entry.dialer,
		})
		reader.SetOffset(startOff)

		perPartLimit := limit - len(messages)
		for i := 0; i < perPartLimit; i++ {
			readCtx, readCancel := context.WithTimeout(ctx, 3*time.Second)
			m, err := reader.ReadMessage(readCtx)
			readCancel()
			if err != nil {
				break
			}
			hdrs := make(map[string]string)
			for _, h := range m.Headers {
				hdrs[h.Key] = string(h.Value)
			}
			messages = append(messages, exportMsg{
				Partition: m.Partition,
				Offset:    m.Offset,
				Timestamp: m.Time.UnixMilli(),
				Key:       string(m.Key),
				Value:     string(m.Value),
				Headers:   hdrs,
			})
		}
		reader.Close()
	}

	format := req.Format
	if format == "" {
		format = "json"
	}

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-export.csv", req.Topic))
		cw := csv.NewWriter(w)
		cw.Write([]string{"partition", "offset", "timestamp", "key", "value"})
		for _, m := range messages {
			cw.Write([]string{
				fmt.Sprintf("%d", m.Partition),
				fmt.Sprintf("%d", m.Offset),
				fmt.Sprintf("%d", m.Timestamp),
				m.Key,
				m.Value,
			})
		}
		cw.Flush()
		return
	}

	// JSON format
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s-export.json", req.Topic))
	json.NewEncoder(w).Encode(messages)
}

// ── Consumer Group endpoints ───────────────────────────────────────────────

// kafkaHandleGroups lists consumer groups.
func (s *Server) kafkaHandleGroups(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	client := entry.kafkaClient()

	// List groups.
	listResp, err := client.ListGroups(ctx, &kafka.ListGroupsRequest{
		Addr: client.Addr,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "list groups failed: " + err.Error()})
		return
	}

	if len(listResp.Groups) == 0 {
		dur := float64(time.Since(start).Milliseconds())
		json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: []any{}})
		return
	}

	// Build a map of protocolType from ListGroups (DescribeGroups doesn't expose it).
	groupIDs := make([]string, len(listResp.Groups))
	protocolTypes := make(map[string]string)
	for i, g := range listResp.Groups {
		groupIDs[i] = g.GroupID
		protocolTypes[g.GroupID] = g.ProtocolType
	}

	descResp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		Addr:     client.Addr,
		GroupIDs: groupIDs,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "describe groups failed: " + err.Error()})
		return
	}

	type groupInfo struct {
		GroupID       string `json:"groupId"`
		State        string `json:"state"`
		ProtocolType string `json:"protocolType"`
		Members      int    `json:"members"`
	}

	search := strings.ToLower(req.Search)
	var groups []groupInfo
	for _, g := range descResp.Groups {
		if g.Error != nil {
			continue
		}
		if search != "" && !strings.Contains(strings.ToLower(g.GroupID), search) {
			continue
		}
		groups = append(groups, groupInfo{
			GroupID:       g.GroupID,
			State:        g.GroupState,
			ProtocolType: protocolTypes[g.GroupID],
			Members:      len(g.Members),
		})
	}

	sort.Slice(groups, func(i, j int) bool { return groups[i].GroupID < groups[j].GroupID })

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: groups})
}

// kafkaHandleGroupDetail returns group members, assignments, and lag.
func (s *Server) kafkaHandleGroupDetail(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Group == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "group name is required"})
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

	// Describe the group.
	descResp, err := client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{
		Addr:     client.Addr,
		GroupIDs: []string{req.Group},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "describe group failed: " + err.Error()})
		return
	}
	if len(descResp.Groups) == 0 {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "group not found"})
		return
	}
	grp := descResp.Groups[0]
	if grp.Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "group error: " + grp.Error.Error()})
		return
	}

	type memberInfo struct {
		MemberID   string   `json:"memberId"`
		ClientID   string   `json:"clientId"`
		ClientHost string   `json:"clientHost"`
		Topics     []string `json:"topics,omitempty"`
	}

	var members []memberInfo
	subscribedTopics := make(map[string]bool)
	for _, m := range grp.Members {
		topics := make([]string, len(m.MemberAssignments.Topics))
		for i, t := range m.MemberAssignments.Topics {
			topics[i] = t.Topic
			subscribedTopics[t.Topic] = true
		}
		members = append(members, memberInfo{
			MemberID:   m.MemberID,
			ClientID:   m.ClientID,
			ClientHost: m.ClientHost,
			Topics:     topics,
		})
	}

	// Fetch committed offsets.
	topicList := make([]string, 0, len(subscribedTopics))
	for t := range subscribedTopics {
		topicList = append(topicList, t)
	}

	offResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr:    client.Addr,
		GroupID: req.Group,
		Topics:  map[string][]int{},
	})

	type lagEntry struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Committed int64  `json:"committed"`
		End       int64  `json:"end"`
		Lag       int64  `json:"lag"`
	}

	var lagEntries []lagEntry
	var totalLag int64

	if err == nil && offResp != nil {
		// Gather all topic-partitions with committed offsets.
		endOffsetReqs := make(map[string][]kafka.OffsetRequest)
		for topic, partOffsets := range offResp.Topics {
			for _, po := range partOffsets {
				if po.CommittedOffset >= 0 {
					endOffsetReqs[topic] = append(endOffsetReqs[topic], kafka.LastOffsetOf(po.Partition))
				}
			}
		}

		var endOffsetsResp *kafka.ListOffsetsResponse
		if len(endOffsetReqs) > 0 {
			endOffsetsResp, _ = client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
				Addr:   client.Addr,
				Topics: endOffsetReqs,
			})
		}

		for topic, partOffsets := range offResp.Topics {
			for _, po := range partOffsets {
				if po.CommittedOffset < 0 {
					continue
				}
				var endOff int64
				if endOffsetsResp != nil {
					if topicOff, ok := endOffsetsResp.Topics[topic]; ok {
						for _, eo := range topicOff {
							if eo.Partition == po.Partition && eo.Error == nil {
								endOff = eo.LastOffset
							}
						}
					}
				}
				lag := endOff - po.CommittedOffset
				if lag < 0 {
					lag = 0
				}
				totalLag += lag
				lagEntries = append(lagEntries, lagEntry{
					Topic:     topic,
					Partition: po.Partition,
					Committed: po.CommittedOffset,
					End:       endOff,
					Lag:       lag,
				})
			}
		}
	}

	sort.Slice(lagEntries, func(i, j int) bool {
		if lagEntries[i].Topic != lagEntries[j].Topic {
			return lagEntries[i].Topic < lagEntries[j].Topic
		}
		return lagEntries[i].Partition < lagEntries[j].Partition
	})

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"groupId":  grp.GroupID,
			"state":    grp.GroupState,
			"members":  members,
			"lag":      lagEntries,
			"totalLag": totalLag,
		},
	})
}

// kafkaHandleGroupResetOffsets resets consumer group offsets.
func (s *Server) kafkaHandleGroupResetOffsets(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Group == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "group name is required"})
		return
	}
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

	// Resolve target offsets per partition.
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   client.Addr,
		Topics: []string{req.Topic},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "metadata failed: " + err.Error()})
		return
	}
	if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic not found or error"})
		return
	}

	partIDs := make([]int, len(meta.Topics[0].Partitions))
	for i, p := range meta.Topics[0].Partitions {
		partIDs[i] = p.ID
	}

	offsets := make(map[int]int64)

	switch req.ResetStrategy {
	case "earliest":
		oReqs := make(map[string][]kafka.OffsetRequest)
		for _, p := range partIDs {
			oReqs[req.Topic] = append(oReqs[req.Topic], kafka.FirstOffsetOf(p))
		}
		oResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: client.Addr, Topics: oReqs})
		if err != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "list offsets failed: " + err.Error()})
			return
		}
		for _, po := range oResp.Topics[req.Topic] {
			if po.Error == nil {
				offsets[po.Partition] = po.FirstOffset
			}
		}
	case "latest":
		oReqs := make(map[string][]kafka.OffsetRequest)
		for _, p := range partIDs {
			oReqs[req.Topic] = append(oReqs[req.Topic], kafka.LastOffsetOf(p))
		}
		oResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Addr: client.Addr, Topics: oReqs})
		if err != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "list offsets failed: " + err.Error()})
			return
		}
		for _, po := range oResp.Topics[req.Topic] {
			if po.Error == nil {
				offsets[po.Partition] = po.LastOffset
			}
		}
	case "offset":
		for _, p := range partIDs {
			offsets[p] = req.Offset
		}
	case "timestamp":
		if req.Timestamp <= 0 {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "timestamp is required for timestamp reset"})
			return
		}
		resolved, err := s.kafkaTimestampToOffsets(ctx, entry, req.Topic, partIDs, req.Timestamp)
		if err != nil {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "timestamp resolve failed: " + err.Error()})
			return
		}
		offsets = resolved
	default:
		json.NewEncoder(w).Encode(KafkaResponse{Error: "resetStrategy must be earliest, latest, offset, or timestamp"})
		return
	}

	// Build the OffsetCommit request.
	topicOffsets := make(map[string][]kafka.OffsetCommit)
	for p, off := range offsets {
		topicOffsets[req.Topic] = append(topicOffsets[req.Topic], kafka.OffsetCommit{
			Partition: p,
			Offset:    off,
		})
	}

	_, err = client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
		Addr:    client.Addr,
		GroupID: req.Group,
		Topics:  topicOffsets,
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "offset commit failed: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"group":      req.Group,
			"topic":      req.Topic,
			"strategy":   req.ResetStrategy,
			"partitions": len(offsets),
		},
	})
}

// kafkaHandleGroupDelete deletes a consumer group.
func (s *Server) kafkaHandleGroupDelete(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Group == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "group name is required"})
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
	resp, err := client.DeleteGroups(ctx, &kafka.DeleteGroupsRequest{
		Addr:     client.Addr,
		GroupIDs: []string{req.Group},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "delete group failed: " + err.Error()})
		return
	}

	if groupErr, ok := resp.Errors[req.Group]; ok && groupErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "delete group error: " + groupErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"deleted": req.Group},
	})
}

// ── Kafka helpers ──────────────────────────────────────────────────────────

// kafkaResolvePartitions returns partition IDs for a topic; if partID >= 0, returns just that one.
func (s *Server) kafkaResolvePartitions(ctx context.Context, entry *kafkaPoolEntry, topic string, partID int) ([]int, error) {
	if partID >= 0 {
		return []int{partID}, nil
	}
	client := entry.kafkaClient()
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   client.Addr,
		Topics: []string{topic},
	})
	if err != nil {
		return nil, fmt.Errorf("metadata failed: %w", err)
	}
	if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
		return nil, fmt.Errorf("topic not found: %s", topic)
	}
	ids := make([]int, len(meta.Topics[0].Partitions))
	for i, p := range meta.Topics[0].Partitions {
		ids[i] = p.ID
	}
	sort.Ints(ids)
	return ids, nil
}

// kafkaTimestampToOffsets resolves a unix-millis timestamp to offsets per partition.
func (s *Server) kafkaTimestampToOffsets(ctx context.Context, entry *kafkaPoolEntry, topic string, partitions []int, tsMillis int64) (map[int]int64, error) {
	client := entry.kafkaClient()
	t := time.UnixMilli(tsMillis)
	offsetReqs := make(map[string][]kafka.OffsetRequest)
	for _, p := range partitions {
		offsetReqs[topic] = append(offsetReqs[topic], kafka.TimeOffsetOf(p, t))
	}
	resp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Addr:   client.Addr,
		Topics: offsetReqs,
	})
	if err != nil {
		return nil, err
	}
	result := make(map[int]int64)
	for _, po := range resp.Topics[topic] {
		if po.Error == nil {
			result[po.Partition] = po.FirstOffset
		}
	}
	return result, nil
}

// ── Topic management endpoints ─────────────────────────────────────────────

// kafkaHandleTopicCreate creates a new topic.
func (s *Server) kafkaHandleTopicCreate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	parts := req.Partitions
	if parts <= 0 {
		parts = 1
	}
	reps := req.Replicas
	if reps <= 0 {
		reps = 1
	}

	configs := make([]kafka.ConfigEntry, 0, len(req.Config))
	for k, v := range req.Config {
		configs = append(configs, kafka.ConfigEntry{ConfigName: k, ConfigValue: v})
	}

	client := entry.kafkaClient()
	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Addr: client.Addr,
		Topics: []kafka.TopicConfig{{
			Topic:             req.Topic,
			NumPartitions:     parts,
			ReplicationFactor: reps,
			ConfigEntries:     configs,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "create topic failed: " + err.Error()})
		return
	}
	if tErr, ok := resp.Errors[req.Topic]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "create topic error: " + tErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":      req.Topic,
			"partitions": parts,
			"replicas":   reps,
		},
	})
}

// kafkaHandleTopicDelete deletes a topic.
func (s *Server) kafkaHandleTopicDelete(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
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
	resp, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{
		Addr:   client.Addr,
		Topics: []string{req.Topic},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "delete topic failed: " + err.Error()})
		return
	}
	if tErr, ok := resp.Errors[req.Topic]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "delete topic error: " + tErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"deleted": req.Topic},
	})
}

// kafkaHandleTopicConfigUpdate alters topic configuration entries.
func (s *Server) kafkaHandleTopicConfigUpdate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
		return
	}
	if len(req.Config) == 0 {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "config entries are required"})
		return
	}

	entry, err := s.getPooledKafkaClientForReq(req)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	entries := make([]kafka.AlterConfigRequestConfig, 0, len(req.Config))
	for k, v := range req.Config {
		entries = append(entries, kafka.AlterConfigRequestConfig{Name: k, Value: v})
	}

	client := entry.kafkaClient()
	resp, err := client.AlterConfigs(ctx, &kafka.AlterConfigsRequest{
		Addr: client.Addr,
		Resources: []kafka.AlterConfigRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: req.Topic,
			Configs:      entries,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "alter config failed: " + err.Error()})
		return
	}
	for res, rErr := range resp.Errors {
		if rErr != nil && res.Name == req.Topic {
			json.NewEncoder(w).Encode(KafkaResponse{Error: "alter config error: " + rErr.Error()})
			return
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":   req.Topic,
			"updated": len(req.Config),
		},
	})
}

// kafkaHandleTopicPartitionsAdd increases the partition count of a topic.
func (s *Server) kafkaHandleTopicPartitionsAdd(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
		return
	}
	if req.Partitions <= 0 {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "partitions must be a positive count (new total)"})
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
	resp, err := client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{
		Addr: client.Addr,
		Topics: []kafka.TopicPartitionsConfig{{
			Name:  req.Topic,
			Count: int32(req.Partitions),
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "add partitions failed: " + err.Error()})
		return
	}
	if tErr, ok := resp.Errors[req.Topic]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "add partitions error: " + tErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":      req.Topic,
			"partitions": req.Partitions,
		},
	})
}

// kafkaHandleTopicPurge purges all messages from a topic by deleting and
// recreating it with the same partition count and configuration. kafka-go
// does not expose the DeleteRecords admin API, so we take this approach as a
// functional equivalent.
func (s *Server) kafkaHandleTopicPurge(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	if req.Topic == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "topic name is required"})
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

	numPartitions, replicationFactor, configs, err := s.kafkaCaptureTopicShape(ctx, client, req.Topic)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	delResp, err := client.DeleteTopics(ctx, &kafka.DeleteTopicsRequest{
		Addr:   client.Addr,
		Topics: []string{req.Topic},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "purge delete failed: " + err.Error()})
		return
	}
	if tErr, ok := delResp.Errors[req.Topic]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "purge delete error: " + tErr.Error()})
		return
	}

	// Wait briefly for the broker to reflect the deletion before recreating.
	if err := s.kafkaWaitForTopicAbsent(ctx, client, req.Topic); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	createResp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Addr: client.Addr,
		Topics: []kafka.TopicConfig{{
			Topic:             req.Topic,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     configs,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "purge recreate failed: " + err.Error()})
		return
	}
	if tErr, ok := createResp.Errors[req.Topic]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "purge recreate error: " + tErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"topic":      req.Topic,
			"partitions": numPartitions,
			"replicas":   replicationFactor,
		},
	})
}

// kafkaHandleTopicClone creates a new topic with the same partition count and
// configuration as an existing topic. Messages are not copied.
func (s *Server) kafkaHandleTopicClone(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	source := req.Topic
	// Reuse DiffTopic2 as the target topic name.
	target := req.DiffTopic2
	if source == "" || target == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "source topic and target topic (diffTopic2) are required"})
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

	numPartitions, replicationFactor, configs, err := s.kafkaCaptureTopicShape(ctx, client, source)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Addr: client.Addr,
		Topics: []kafka.TopicConfig{{
			Topic:             target,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			ConfigEntries:     configs,
		}},
	})
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "clone create failed: " + err.Error()})
		return
	}
	if tErr, ok := resp.Errors[target]; ok && tErr != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "clone create error: " + tErr.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"source":     source,
			"target":     target,
			"partitions": numPartitions,
			"replicas":   replicationFactor,
		},
	})
}

// kafkaCaptureTopicShape returns the partition count, replication factor, and
// non-default, non-read-only config entries for a topic so it can be
// recreated or cloned.
func (s *Server) kafkaCaptureTopicShape(ctx context.Context, client *kafka.Client, topic string) (int, int, []kafka.ConfigEntry, error) {
	meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
		Addr:   client.Addr,
		Topics: []string{topic},
	})
	if err != nil {
		return 0, 0, nil, fmt.Errorf("metadata fetch failed: %w", err)
	}
	if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
		return 0, 0, nil, fmt.Errorf("topic not found: %s", topic)
	}

	tp := meta.Topics[0]
	numPartitions := len(tp.Partitions)
	replicationFactor := 1
	if numPartitions > 0 {
		replicationFactor = len(tp.Partitions[0].Replicas)
	}

	configResp, err := client.DescribeConfigs(ctx, &kafka.DescribeConfigsRequest{
		Addr: client.Addr,
		Resources: []kafka.DescribeConfigRequestResource{{
			ResourceType: kafka.ResourceTypeTopic,
			ResourceName: topic,
		}},
	})

	var configs []kafka.ConfigEntry
	if err == nil {
		for _, res := range configResp.Resources {
			if res.Error != nil {
				continue
			}
			for _, e := range res.ConfigEntries {
				if e.IsDefault || e.ReadOnly || e.IsSensitive {
					continue
				}
				configs = append(configs, kafka.ConfigEntry{
					ConfigName:  e.ConfigName,
					ConfigValue: e.ConfigValue,
				})
			}
		}
	}

	return numPartitions, replicationFactor, configs, nil
}

// kafkaWaitForTopicAbsent polls metadata until the topic is no longer
// present, with a short timeout. Used between delete+recreate for purge.
func (s *Server) kafkaWaitForTopicAbsent(ctx context.Context, client *kafka.Client, topic string) error {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := client.Metadata(ctx, &kafka.MetadataRequest{
			Addr:   client.Addr,
			Topics: []string{topic},
		})
		if err != nil {
			return fmt.Errorf("metadata poll failed: %w", err)
		}
		if len(meta.Topics) == 0 || meta.Topics[0].Error != nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("topic %s still present after delete; broker has delete.topic.enable=false or deletion is slow", topic)
}

// ── Schema Registry endpoints ──────────────────────────────────────────────

// schemaRegistryRequest performs an HTTP request against the Schema Registry
// configured on the connection and returns the raw response body and status.
func (s *Server) schemaRegistryRequest(conn KafkaConnection, method, path string, body any) (int, []byte, error) {
	base := strings.TrimRight(conn.SchemaRegistryURL, "/")
	if base == "" {
		return 0, nil, fmt.Errorf("schemaRegistryUrl is not configured on the connection")
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
	hreq.Header.Set("Accept", "application/vnd.schemaregistry.v1+json, application/json")
	if body != nil {
		hreq.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
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

// writeSchemaRegistryError decodes a Schema Registry error body ({error_code, message})
// or falls back to the raw body.
func writeSchemaRegistryError(w http.ResponseWriter, status int, body []byte, start time.Time) {
	msg := strings.TrimSpace(string(body))
	var sr struct {
		ErrorCode int    `json:"error_code"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(body, &sr) == nil && sr.Message != "" {
		msg = fmt.Sprintf("[%d] %s", sr.ErrorCode, sr.Message)
	}
	if msg == "" {
		msg = fmt.Sprintf("HTTP %d", status)
	}
	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{Error: msg, DurationMs: dur})
}

// kafkaHandleSchemas lists all Schema Registry subjects.
func (s *Server) kafkaHandleSchemas(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	status, body, err := s.schemaRegistryRequest(req.Connection, http.MethodGet, "/subjects", nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, body, start)
		return
	}
	var subjects []string
	if err := json.Unmarshal(body, &subjects); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode subjects: " + err.Error()})
		return
	}
	sort.Strings(subjects)

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data:       map[string]any{"subjects": subjects},
	})
}

// kafkaHandleSchemaDetail lists all versions for a subject and returns the
// latest version content.
func (s *Server) kafkaHandleSchemaDetail(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if req.Subject == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "subject is required"})
		return
	}

	versPath := fmt.Sprintf("/subjects/%s/versions", req.Subject)
	status, body, err := s.schemaRegistryRequest(req.Connection, http.MethodGet, versPath, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, body, start)
		return
	}
	var versions []int
	if err := json.Unmarshal(body, &versions); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode versions: " + err.Error()})
		return
	}
	sort.Ints(versions)

	// Fetch latest version content for convenience.
	var latest map[string]any
	if len(versions) > 0 {
		latestPath := fmt.Sprintf("/subjects/%s/versions/latest", req.Subject)
		lStatus, lBody, lErr := s.schemaRegistryRequest(req.Connection, http.MethodGet, latestPath, nil)
		if lErr == nil && lStatus < 300 {
			_ = json.Unmarshal(lBody, &latest)
		}
	}

	// Try to fetch subject-level compatibility config (may 404 if not set).
	var compat string
	cStatus, cBody, cErr := s.schemaRegistryRequest(req.Connection, http.MethodGet, "/config/"+req.Subject, nil)
	if cErr == nil && cStatus < 300 {
		var cfg struct {
			CompatibilityLevel string `json:"compatibilityLevel"`
		}
		if json.Unmarshal(cBody, &cfg) == nil {
			compat = cfg.CompatibilityLevel
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"subject":       req.Subject,
			"versions":      versions,
			"latest":        latest,
			"compatibility": compat,
		},
	})
}

// kafkaHandleSchemaVersion returns a specific schema version's content.
func (s *Server) kafkaHandleSchemaVersion(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if req.Subject == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "subject is required"})
		return
	}
	version := "latest"
	if req.SchemaVersion > 0 {
		version = fmt.Sprintf("%d", req.SchemaVersion)
	}
	path := fmt.Sprintf("/subjects/%s/versions/%s", req.Subject, version)
	status, body, err := s.schemaRegistryRequest(req.Connection, http.MethodGet, path, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, body, start)
		return
	}
	var data map[string]any
	if err := json.Unmarshal(body, &data); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode version: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

// kafkaHandleSchemaCreate registers a new schema version under a subject.
func (s *Server) kafkaHandleSchemaCreate(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if req.Subject == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "subject is required"})
		return
	}
	if strings.TrimSpace(req.SchemaContent) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "schema content is required"})
		return
	}

	schemaType := req.SchemaType
	if schemaType == "" {
		schemaType = "AVRO"
	}
	body := map[string]any{
		"schema":     req.SchemaContent,
		"schemaType": strings.ToUpper(schemaType),
	}

	path := fmt.Sprintf("/subjects/%s/versions", req.Subject)
	status, respBody, err := s.schemaRegistryRequest(req.Connection, http.MethodPost, path, body)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, respBody, start)
		return
	}
	var data map[string]any
	if err := json.Unmarshal(respBody, &data); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode register response: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

// kafkaHandleSchemaDelete deletes a subject or a specific version.
func (s *Server) kafkaHandleSchemaDelete(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if req.Subject == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "subject is required"})
		return
	}

	var path string
	if req.SchemaVersion > 0 {
		path = fmt.Sprintf("/subjects/%s/versions/%d", req.Subject, req.SchemaVersion)
	} else {
		path = fmt.Sprintf("/subjects/%s", req.Subject)
	}
	status, respBody, err := s.schemaRegistryRequest(req.Connection, http.MethodDelete, path, nil)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, respBody, start)
		return
	}
	var data any
	_ = json.Unmarshal(respBody, &data)

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{
		DurationMs: dur,
		Data: map[string]any{
			"subject": req.Subject,
			"version": req.SchemaVersion,
			"result":  data,
		},
	})
}

// kafkaHandleSchemaCompatibility runs a compatibility check for a candidate
// schema against the latest or specified version of a subject.
func (s *Server) kafkaHandleSchemaCompatibility(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()
	if req.Subject == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "subject is required"})
		return
	}
	if strings.TrimSpace(req.SchemaContent) == "" {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "schema content is required"})
		return
	}

	schemaType := req.SchemaType
	if schemaType == "" {
		schemaType = "AVRO"
	}
	body := map[string]any{
		"schema":     req.SchemaContent,
		"schemaType": strings.ToUpper(schemaType),
	}

	version := "latest"
	if req.SchemaVersion > 0 {
		version = fmt.Sprintf("%d", req.SchemaVersion)
	}
	path := fmt.Sprintf("/compatibility/subjects/%s/versions/%s", req.Subject, version)
	status, respBody, err := s.schemaRegistryRequest(req.Connection, http.MethodPost, path, body)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, respBody, start)
		return
	}
	var data map[string]any
	if err := json.Unmarshal(respBody, &data); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode compatibility: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

// kafkaHandleSchemaConfig GETs or PUTs compatibility config globally or per-subject.
func (s *Server) kafkaHandleSchemaConfig(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	path := "/config"
	if req.Subject != "" {
		path = "/config/" + req.Subject
	}

	method := http.MethodGet
	var body any
	if strings.TrimSpace(req.Compatibility) != "" {
		method = http.MethodPut
		body = map[string]any{"compatibility": strings.ToUpper(req.Compatibility)}
	}

	status, respBody, err := s.schemaRegistryRequest(req.Connection, method, path, body)
	if err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: err.Error()})
		return
	}
	if status >= 300 {
		writeSchemaRegistryError(w, status, respBody, start)
		return
	}
	var data map[string]any
	if err := json.Unmarshal(respBody, &data); err != nil {
		json.NewEncoder(w).Encode(KafkaResponse{Error: "decode config: " + err.Error()})
		return
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(KafkaResponse{DurationMs: dur, Data: data})
}

