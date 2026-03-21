package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// elasticHit is the per-document shape returned by the Elasticsearch Search API.
type elasticHit struct {
	Index  string         `json:"_index"`
	ID     string         `json:"_id"`
	Score  *float64       `json:"_score"`
	Source map[string]any `json:"_source"`
}

// handleElasticGateway is the entry-point for all Elasticsearch gateway requests.
// It strips the gateway routing headers and dispatches by r.URL.Path.
func handleElasticGateway(w http.ResponseWriter, r *http.Request) {
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
		handleElasticTest(w, r, req)
	case "query":
		handleElasticQuery(w, r, req)
	case "tables":
		handleElasticIndices(w, r, req)
	case "describe":
		handleElasticDescribe(w, r, req)
	case "databases":
		handleElasticDatabases(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(dbResponse{Error: "unknown gateway endpoint: " + path})
	}
}

func handleElasticTest(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledElasticClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	res, err := client.Ping(client.Ping.WithContext(ctx))
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		json.NewEncoder(w).Encode(dbResponse{Error: res.String(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(dbResponse{Duration: ms(t0)})
}

// handleElasticQuery executes an Elasticsearch search against an index.
// req.Table = index name, req.SQL = JSON query body (e.g. {"query":{"match_all":{}}}).
func handleElasticQuery(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "index is required", Duration: ms(t0)})
		return
	}

	client, err := getPooledElasticClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	queryBody := strings.TrimSpace(req.SQL)
	if queryBody == "" || queryBody == "{}" {
		queryBody = `{"query":{"match_all":{}}}`
	}

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(req.Table),
		client.Search.WithBody(strings.NewReader(queryBody)),
		client.Search.WithSize(req.Limit),
		client.Search.WithTrackTotalHits(true),
	)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		var errBody map[string]any
		json.NewDecoder(res.Body).Decode(&errBody) //nolint:errcheck
		msg := fmt.Sprintf("[%s] %v", res.Status(), errBody)
		json.NewEncoder(w).Encode(dbResponse{Error: msg, Duration: ms(t0)})
		return
	}

	var searchResult struct {
		Hits struct {
			Total struct {
				Value int `json:"value"`
			} `json:"total"`
			Hits []elasticHit `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(res.Body).Decode(&searchResult); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: "decode error: " + err.Error(), Duration: ms(t0)})
		return
	}

	columns, rows := elasticHitsToColumnsRows(searchResult.Hits.Hits)
	json.NewEncoder(w).Encode(dbResponse{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Duration: ms(t0),
	})
}

// handleElasticIndices lists all non-system indices.
func handleElasticIndices(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledElasticClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	res, err := client.Cat.Indices(
		client.Cat.Indices.WithContext(ctx),
		client.Cat.Indices.WithFormat("json"),
		client.Cat.Indices.WithH("index,docs.count,store.size,status"),
		client.Cat.Indices.WithS("index"),
	)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		json.NewEncoder(w).Encode(dbResponse{Error: res.String(), Duration: ms(t0)})
		return
	}

	var catResult []struct {
		Index  string `json:"index"`
		Status string `json:"status"`
	}
	if err := json.NewDecoder(res.Body).Decode(&catResult); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: "decode error: " + err.Error(), Duration: ms(t0)})
		return
	}

	tables := make([]dbTable, 0, len(catResult))
	for _, idx := range catResult {
		// Skip internal/system indices (prefixed with ".").
		if strings.HasPrefix(idx.Index, ".") {
			continue
		}
		tables = append(tables, dbTable{Name: idx.Index, Type: "index"})
	}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// handleElasticDescribe returns the field mappings for an index as columns.
func handleElasticDescribe(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "index is required", Duration: ms(t0)})
		return
	}

	client, err := getPooledElasticClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithContext(ctx),
		client.Indices.GetMapping.WithIndex(req.Table),
	)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		json.NewEncoder(w).Encode(dbResponse{Error: res.String(), Duration: ms(t0)})
		return
	}

	// Mapping response: {indexName: {mappings: {properties: {field: {type: ...}}}}}
	var mappingResult map[string]struct {
		Mappings struct {
			Properties map[string]map[string]any `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&mappingResult); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: "decode error: " + err.Error(), Duration: ms(t0)})
		return
	}

	// Merge properties from all matching index names (handles aliases/wildcards).
	allProps := make(map[string]string)
	for _, idxMapping := range mappingResult {
		for fieldName, fieldDef := range idxMapping.Mappings.Properties {
			if typ, ok := fieldDef["type"].(string); ok {
				allProps[fieldName] = typ
			} else {
				allProps[fieldName] = "object"
			}
		}
	}

	fieldNames := make([]string, 0, len(allProps))
	for name := range allProps {
		fieldNames = append(fieldNames, name)
	}
	sort.Strings(fieldNames)

	columns := make([]dbColumn, len(fieldNames))
	for i, name := range fieldNames {
		columns[i] = dbColumn{Name: name, Type: allProps[name], Nullable: true}
	}
	json.NewEncoder(w).Encode(dbResponse{Columns: columns, Duration: ms(t0)})
}

// handleElasticDatabases returns the cluster name as a single "database" entry.
// Elasticsearch has no database concept — this provides parity with the other gateways.
func handleElasticDatabases(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledElasticClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	res, err := client.Info(client.Info.WithContext(ctx))
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer res.Body.Close()

	if res.IsError() {
		json.NewEncoder(w).Encode(dbResponse{Error: res.String(), Duration: ms(t0)})
		return
	}

	var info struct {
		ClusterName string `json:"cluster_name"`
	}
	json.NewDecoder(res.Body).Decode(&info) //nolint:errcheck

	name := info.ClusterName
	if name == "" {
		name = "elasticsearch"
	}
	tables := []dbTable{{Name: name, Type: "cluster"}}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// elasticHitsToColumnsRows converts Elasticsearch search hits to the standard columns/rows format.
// _id is always placed first; source fields follow in alphabetical order.
func elasticHitsToColumnsRows(hits []elasticHit) ([]dbColumn, [][]any) {
	if len(hits) == 0 {
		return []dbColumn{}, [][]any{}
	}

	// Collect unique source field names across all hits.
	seen := make(map[string]bool)
	var sourceKeys []string
	for _, hit := range hits {
		for k := range hit.Source {
			if !seen[k] {
				seen[k] = true
				sourceKeys = append(sourceKeys, k)
			}
		}
	}
	sort.Strings(sourceKeys)

	keys := append([]string{"_id"}, sourceKeys...)
	columns := make([]dbColumn, len(keys))
	for i, k := range keys {
		typ := "keyword"
		if k != "_id" {
			typ = "mixed"
		}
		columns[i] = dbColumn{Name: k, Type: typ}
	}

	rows := make([][]any, len(hits))
	for i, hit := range hits {
		row := make([]any, len(keys))
		for j, k := range keys {
			if k == "_id" {
				row[j] = hit.ID
			} else {
				row[j] = hit.Source[k]
			}
		}
		rows[i] = row
	}
	return columns, rows
}
