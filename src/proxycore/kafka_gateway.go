package proxycore

import (
	"context"
	"encoding/json"
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
	default:
		json.NewEncoder(w).Encode(KafkaResponse{Error: "unknown endpoint: " + path})
	}
}

// kafkaHandleTest dials the first broker and verifies connectivity.
func (s *Server) kafkaHandleTest(w http.ResponseWriter, req KafkaRequest) {
	start := time.Now()

	entry, err := s.getPooledKafkaClient(req.Connection)
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

	entry, err := s.getPooledKafkaClient(req.Connection)
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

	entry, err := s.getPooledKafkaClient(req.Connection)
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

	entry, err := s.getPooledKafkaClient(req.Connection)
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

	entry, err := s.getPooledKafkaClient(req.Connection)
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

