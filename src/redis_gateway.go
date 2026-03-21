package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"

	"github.com/redis/go-redis/v9"
)

// handleRedisGateway is the entry-point for all Redis gateway requests.
func handleRedisGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(dbResponse{Error: "only POST is accepted"})
		return
	}

	var req dbRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(dbResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	if req.Limit <= 0 {
		req.Limit = defaultRowLimit
	}
	if req.Limit > maxRowLimit {
		req.Limit = maxRowLimit
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "test":
		redisHandleTest(w, req)
	case "query":
		redisHandleQuery(w, req)
	case "tables":
		redisHandleKeys(w, req)
	case "describe":
		redisHandleDescribe(w, req)
	case "databases":
		redisHandleDatabases(w, req)
	default:
		json.NewEncoder(w).Encode(dbResponse{Error: "unknown endpoint: " + path})
	}
}

func redisHandleTest(w http.ResponseWriter, req dbRequest) {
	client, err := getPooledRedisClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: "PING failed: " + err.Error()})
		return
	}

	json.NewEncoder(w).Encode(dbResponse{
		Columns:  []dbColumn{{Name: "result"}},
		Rows:     [][]any{{"PONG"}},
		RowCount: 1,
	})
}

func redisHandleQuery(w http.ResponseWriter, req dbRequest) {
	client, err := getPooledRedisClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}

	cmd := strings.TrimSpace(req.SQL)
	if cmd == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "no command provided"})
		return
	}

	args := parseRedisArgs(cmd)
	if len(args) == 0 {
		json.NewEncoder(w).Encode(dbResponse{Error: "empty command"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	start := time.Now()
	result := client.Do(ctx, args...)
	dur := float64(time.Since(start).Milliseconds())

	val, err := result.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			json.NewEncoder(w).Encode(dbResponse{
				Columns:  []dbColumn{{Name: "result"}},
				Rows:     [][]any{{"(nil)"}},
				RowCount: 1,
				Duration: dur,
			})
			return
		}
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: dur})
		return
	}

	cmdName := strings.ToUpper(args[0].(string))
	resp := formatRedisVal(val, cmdName)
	resp.Duration = dur
	json.NewEncoder(w).Encode(resp)
}

// redisHandleKeys scans keys matching the pattern in req.SQL (default "*").
func redisHandleKeys(w http.ResponseWriter, req dbRequest) {
	client, err := getPooledRedisClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}

	pattern := strings.TrimSpace(req.SQL)
	if pattern == "" {
		pattern = "*"
	}

	ctx, cancel := context.WithTimeout(context.Background(), queryTimeout)
	defer cancel()

	start := time.Now()
	var keys []string
	iter := client.Scan(ctx, 0, pattern, int64(req.Limit)).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
		if len(keys) >= req.Limit {
			break
		}
	}
	dur := float64(time.Since(start).Milliseconds())

	if err := iter.Err(); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: dur})
		return
	}

	tables := make([]dbTable, len(keys))
	rows := make([][]any, len(keys))
	for i, k := range keys {
		tables[i] = dbTable{Name: k, Type: "key"}
		rows[i] = []any{k}
	}
	json.NewEncoder(w).Encode(dbResponse{
		Tables:   tables,
		Columns:  []dbColumn{{Name: "key"}},
		Rows:     rows,
		RowCount: len(rows),
		Duration: dur,
	})
}

// redisHandleDescribe returns type, TTL, and a value preview for req.Table (the key).
func redisHandleDescribe(w http.ResponseWriter, req dbRequest) {
	client, err := getPooledRedisClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}

	key := strings.TrimSpace(req.Table)
	if key == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "no key specified"})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	start := time.Now()
	keyType, err := client.Type(ctx, key).Result()
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}
	if keyType == "none" {
		json.NewEncoder(w).Encode(dbResponse{Error: fmt.Sprintf("key %q does not exist", key)})
		return
	}

	ttl, _ := client.TTL(ctx, key).Result()

	cols := []dbColumn{{Name: "property"}, {Name: "value"}}
	rows := [][]any{
		{"type", keyType},
		{"ttl", ttl.String()},
	}

	switch keyType {
	case "string":
		val, _ := client.Get(ctx, key).Result()
		rows = append(rows, []any{"value", val})
	case "list":
		length, _ := client.LLen(ctx, key).Result()
		rows = append(rows, []any{"length", length})
		vals, _ := client.LRange(ctx, key, 0, 9).Result()
		for i, v := range vals {
			rows = append(rows, []any{fmt.Sprintf("[%d]", i), v})
		}
	case "hash":
		fields, _ := client.HGetAll(ctx, key).Result()
		rows = append(rows, []any{"fields", len(fields)})
		for f, v := range fields {
			rows = append(rows, []any{f, v})
		}
	case "set":
		size, _ := client.SCard(ctx, key).Result()
		rows = append(rows, []any{"size", size})
		members, _ := client.SRandMemberN(ctx, key, 10).Result()
		for _, m := range members {
			rows = append(rows, []any{"member", m})
		}
	case "zset":
		size, _ := client.ZCard(ctx, key).Result()
		rows = append(rows, []any{"size", size})
		members, _ := client.ZRangeWithScores(ctx, key, 0, 9).Result()
		for _, m := range members {
			rows = append(rows, []any{m.Member, m.Score})
		}
	}

	dur := float64(time.Since(start).Milliseconds())
	json.NewEncoder(w).Encode(dbResponse{
		Columns:  cols,
		Rows:     rows,
		RowCount: len(rows),
		Duration: dur,
	})
}

// redisHandleDatabases returns the available DB indices (0 to databases-1).
func redisHandleDatabases(w http.ResponseWriter, req dbRequest) {
	client, err := getPooledRedisClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	numDBs := 16 // Redis default
	cfg, err := client.ConfigGet(ctx, "databases").Result()
	if err == nil {
		if dbsVal, ok := cfg["databases"]; ok {
			fmt.Sscanf(dbsVal, "%d", &numDBs)
		}
	}

	tables := make([]dbTable, numDBs)
	rows := make([][]any, numDBs)
	for i := 0; i < numDBs; i++ {
		name := fmt.Sprintf("db%d", i)
		tables[i] = dbTable{Name: name, Type: "database"}
		rows[i] = []any{name}
	}

	json.NewEncoder(w).Encode(dbResponse{
		Tables:   tables,
		Columns:  []dbColumn{{Name: "database"}},
		Rows:     rows,
		RowCount: len(rows),
	})
}

// parseRedisArgs splits a Redis command string into arguments,
// respecting single- and double-quoted tokens.
func parseRedisArgs(cmd string) []any {
	var args []any
	var cur strings.Builder
	var inQuote rune

	for _, ch := range cmd {
		switch {
		case inQuote != 0:
			if ch == inQuote {
				inQuote = 0
			} else {
				cur.WriteRune(ch)
			}
		case ch == '\'' || ch == '"':
			inQuote = ch
		case unicode.IsSpace(ch):
			if cur.Len() > 0 {
				args = append(args, cur.String())
				cur.Reset()
			}
		default:
			cur.WriteRune(ch)
		}
	}
	if cur.Len() > 0 {
		args = append(args, cur.String())
	}
	return args
}

// formatRedisVal converts a raw Redis reply value to a dbResponse.
func formatRedisVal(val any, cmdName string) dbResponse {
	switch v := val.(type) {
	case string:
		return dbResponse{
			Columns:  []dbColumn{{Name: "result"}},
			Rows:     [][]any{{v}},
			RowCount: 1,
		}
	case int64:
		return dbResponse{
			Columns:  []dbColumn{{Name: "result"}},
			Rows:     [][]any{{v}},
			RowCount: 1,
		}
	case []any:
		// HGETALL returns a flat [field, val, field, val, ...] array.
		if cmdName == "HGETALL" && len(v)%2 == 0 {
			cols := []dbColumn{{Name: "field"}, {Name: "value"}}
			rows := make([][]any, len(v)/2)
			for i := range rows {
				rows[i] = []any{v[i*2], v[i*2+1]}
			}
			return dbResponse{Columns: cols, Rows: rows, RowCount: len(rows)}
		}
		cols := []dbColumn{{Name: "value"}}
		rows := make([][]any, len(v))
		for i, item := range v {
			rows[i] = []any{item}
		}
		return dbResponse{Columns: cols, Rows: rows, RowCount: len(rows)}
	case map[any]any:
		// RESP3 map reply (Redis 7+ with HELLO 3)
		cols := []dbColumn{{Name: "field"}, {Name: "value"}}
		rows := make([][]any, 0, len(v))
		for k, mv := range v {
			rows = append(rows, []any{k, mv})
		}
		return dbResponse{Columns: cols, Rows: rows, RowCount: len(rows)}
	case nil:
		return dbResponse{
			Columns:  []dbColumn{{Name: "result"}},
			Rows:     [][]any{{"(nil)"}},
			RowCount: 1,
		}
	default:
		return dbResponse{
			Columns:  []dbColumn{{Name: "result"}},
			Rows:     [][]any{{fmt.Sprintf("%v", val)}},
			RowCount: 1,
		}
	}
}
