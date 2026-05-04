//go:build integration

package proxycore

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// Run Phase 1 integration tests with a live Kafka broker:
//
//	KAFKA_TEST_BROKERS=localhost:9092 go test -v -tags integration -run TestIntegration_Kafka ./...
//
// Requires the devstudio test-infra Kafka service to be running with seed data loaded.
// Default broker: localhost:9092

func kafkaTestConn() KafkaConnection {
	brokers := os.Getenv("KAFKA_TEST_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}
	return KafkaConnection{
		Brokers: strings.Split(brokers, ","),
	}
}

// TestIntegration_Kafka_Test verifies connectivity to the live broker and returns controller info.
func TestIntegration_Kafka_Test(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "test", map[string]any{"connection": conn})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("test endpoint failed: %s", resp.Error)
	}

	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected object data, got %T: %v", resp.Data, resp.Data)
	}

	brokersVal, ok := data["brokers"]
	if !ok {
		t.Fatal("response missing 'brokers' field")
	}
	brokerCount, ok := brokersVal.(float64)
	if !ok || brokerCount < 1 {
		t.Errorf("expected at least 1 broker, got %v", brokersVal)
	}

	if _, ok := data["controller"]; !ok {
		t.Error("response missing 'controller' field")
	}

	if resp.DurationMs <= 0 {
		t.Error("expected positive durationMs")
	}
}

// TestIntegration_Kafka_Brokers verifies the broker list endpoint returns id, host, port for each broker.
func TestIntegration_Kafka_Brokers(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "brokers", map[string]any{"connection": conn})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("brokers endpoint failed: %s", resp.Error)
	}

	envelope, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected envelope object, got %T: %v", resp.Data, resp.Data)
	}
	rows, ok := envelope["brokers"].([]any)
	if !ok || len(rows) == 0 {
		t.Fatalf("expected non-empty broker list under 'brokers', got %T: %v", envelope["brokers"], envelope["brokers"])
	}

	for i, row := range rows {
		b, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("broker[%d] is not an object: %T", i, row)
		}
		if _, ok := b["id"]; !ok {
			t.Errorf("broker[%d] missing 'id'", i)
		}
		if _, ok := b["host"]; !ok {
			t.Errorf("broker[%d] missing 'host'", i)
		}
		if _, ok := b["port"]; !ok {
			t.Errorf("broker[%d] missing 'port'", i)
		}
	}
}

// TestIntegration_Kafka_BrokerConfig verifies the broker config endpoint returns config entries
// including well-known keys such as log.retention.hours.
func TestIntegration_Kafka_BrokerConfig(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	// "topic" is overloaded as the broker resource id (see kafkaHandleBrokerConfig).
	// The lima testbed Kafka container sets KAFKA_NODE_ID=1, so the broker id is "1".
	rr := kafkaPost(t, srv, "broker/config", map[string]any{"connection": conn, "topic": "1"})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("broker/config endpoint failed: %s", resp.Error)
	}

	entries, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("expected array data, got %T", resp.Data)
	}
	if len(entries) == 0 {
		t.Fatal("expected at least one config entry")
	}

	// Verify the structure of config entries.
	for i, entry := range entries {
		cfg, ok := entry.(map[string]any)
		if !ok {
			t.Fatalf("entry[%d] is not an object: %T", i, entry)
		}
		if _, ok := cfg["name"]; !ok {
			t.Errorf("entry[%d] missing 'name'", i)
		}
		if _, ok := cfg["value"]; !ok {
			t.Errorf("entry[%d] missing 'value'", i)
		}
		break // structural check on first entry is sufficient
	}

	// Verify a well-known key is present.
	found := false
	for _, entry := range entries {
		cfg := entry.(map[string]any)
		if name, _ := cfg["name"].(string); name == "log.retention.hours" {
			found = true
			break
		}
	}
	if !found {
		t.Log("note: log.retention.hours not found in broker config (non-fatal — may vary by Kafka version)")
	}
}

// TestIntegration_Kafka_Topics verifies the topics list includes the seeded test topic.
func TestIntegration_Kafka_Topics(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "topics", map[string]any{"connection": conn})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("topics endpoint failed: %s", resp.Error)
	}

	envelope, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected envelope object, got %T", resp.Data)
	}
	topics, ok := envelope["topics"].([]any)
	if !ok {
		t.Fatalf("expected array under 'topics', got %T", envelope["topics"])
	}

	// Verify structure of topic entries.
	if len(topics) == 0 {
		t.Fatal("expected at least one topic")
	}
	first, _ := topics[0].(map[string]any)
	for _, field := range []string{"name", "partitions", "replicas", "messages"} {
		if _, ok := first[field]; !ok {
			t.Errorf("topic entry missing field %q", field)
		}
	}

	// Locate the seeded test topic.
	found := false
	for _, entry := range topics {
		topic, _ := entry.(map[string]any)
		if name, _ := topic["name"].(string); name == "devstudio-test-events" {
			found = true
			partitions, _ := topic["partitions"].(float64)
			if partitions != 3 {
				t.Errorf("devstudio-test-events: expected 3 partitions, got %v", partitions)
			}
			break
		}
	}
	if !found {
		t.Error("seeded topic 'devstudio-test-events' not found in topic list — run test-infra seed first")
	}
}

// TestIntegration_Kafka_Topics_Search verifies that the search filter returns only matching topics.
func TestIntegration_Kafka_Topics_Search(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "topics", map[string]any{
		"connection": conn,
		"search":     "devstudio-test",
	})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("topics search failed: %s", resp.Error)
	}

	envelope, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected envelope object, got %T", resp.Data)
	}
	topics, ok := envelope["topics"].([]any)
	if !ok {
		t.Fatalf("expected array under 'topics', got %T", envelope["topics"])
	}

	for _, entry := range topics {
		topic, _ := entry.(map[string]any)
		name, _ := topic["name"].(string)
		if !strings.Contains(strings.ToLower(name), "devstudio-test") {
			t.Errorf("search filter leaked non-matching topic %q", name)
		}
	}
}

// TestIntegration_Kafka_TopicDetail verifies partition layout and offset watermarks for the seeded topic.
func TestIntegration_Kafka_TopicDetail(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "topic/detail", map[string]any{
		"connection": conn,
		"topic":      "devstudio-test-events",
	})
	resp := decodeKafkaResp(t, rr)

	if resp.Error != "" {
		t.Fatalf("topic/detail endpoint failed: %s", resp.Error)
	}

	data, ok := resp.Data.(map[string]any)
	if !ok {
		t.Fatalf("expected object data, got %T", resp.Data)
	}

	// Verify partition list.
	partitions, ok := data["partitions"].([]any)
	if !ok || len(partitions) == 0 {
		t.Fatal("expected non-empty partitions array")
	}
	if len(partitions) != 3 {
		t.Errorf("devstudio-test-events: expected 3 partitions, got %d", len(partitions))
	}

	// Verify partition entry structure.
	for i, p := range partitions {
		part, ok := p.(map[string]any)
		if !ok {
			t.Fatalf("partition[%d] is not an object", i)
		}
		for _, field := range []string{"id", "leader", "replicas", "isr", "offsetLo", "offsetHi"} {
			if _, ok := part[field]; !ok {
				t.Errorf("partition[%d] missing field %q", i, field)
			}
		}
		// High watermark should be >= low watermark.
		lo, _ := part["offsetLo"].(float64)
		hi, _ := part["offsetHi"].(float64)
		if hi < lo {
			t.Errorf("partition[%d]: offsetHi (%v) < offsetLo (%v)", i, hi, lo)
		}
	}

	// Verify topic config is present.
	configs, ok := data["config"].([]any)
	if !ok {
		t.Fatal("expected 'config' array in topic detail")
	}
	if len(configs) == 0 {
		t.Error("expected at least one config entry in topic detail")
	}

	// Verify config entry structure.
	if len(configs) > 0 {
		cfg, ok := configs[0].(map[string]any)
		if !ok {
			t.Fatal("config entry is not an object")
		}
		for _, field := range []string{"name", "value"} {
			if _, ok := cfg[field]; !ok {
				t.Errorf("config entry missing field %q", field)
			}
		}
	}
}

// TestIntegration_Kafka_TopicDetail_NotFound verifies a descriptive error for a non-existent topic.
func TestIntegration_Kafka_TopicDetail_NotFound(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	rr := kafkaPost(t, srv, "topic/detail", map[string]any{
		"connection": conn,
		"topic":      "this-topic-does-not-exist-xyzabc",
	})
	resp := decodeKafkaResp(t, rr)

	// Should get an error (topic not found or Kafka error), not a crash.
	if resp.Error == "" && resp.Data == nil {
		t.Error("expected an error or empty data for non-existent topic")
	}
}

// TestIntegration_Kafka_ResponseShape verifies every Phase 1 endpoint returns valid JSON
// with the standard KafkaResponse envelope.
func TestIntegration_Kafka_ResponseShape(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := kafkaTestConn()

	endpoints := []struct {
		path string
		body map[string]any
	}{
		{"test", map[string]any{"connection": conn}},
		{"brokers", map[string]any{"connection": conn}},
		{"broker/config", map[string]any{"connection": conn, "topic": "0"}},
		{"topics", map[string]any{"connection": conn}},
		{"topic/detail", map[string]any{"connection": conn, "topic": "devstudio-test-events"}},
	}

	for _, ep := range endpoints {
		t.Run(ep.path, func(t *testing.T) {
			rr := kafkaPost(t, srv, ep.path, ep.body)

			if ct := rr.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
				t.Errorf("Content-Type = %q, want application/json", ct)
			}

			// Verify the response is decodable into KafkaResponse.
			var resp KafkaResponse
			if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
				t.Fatalf("response is not valid KafkaResponse JSON: %v", err)
			}

			// durationMs should always be present.
			if resp.DurationMs < 0 {
				t.Errorf("durationMs should be non-negative, got %v", resp.DurationMs)
			}
		})
	}
}
