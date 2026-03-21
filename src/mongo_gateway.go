package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// handleMongoGateway is the entry-point for all MongoDB gateway requests.
// It strips the gateway routing headers and dispatches by r.URL.Path.
func handleMongoGateway(w http.ResponseWriter, r *http.Request) {
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
		handleMongoTest(w, r, req)
	case "query":
		handleMongoQuery(w, r, req)
	case "tables":
		handleMongoCollections(w, r, req)
	case "describe":
		handleMongoDescribe(w, r, req)
	case "databases":
		handleMongoDatabases(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(dbResponse{Error: "unknown gateway endpoint: " + path})
	}
}

func handleMongoTest(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(dbResponse{Duration: ms(t0)})
}

// handleMongoQuery executes a find against a collection.
// req.Table = collection name, req.SQL = JSON filter (e.g. {"status":"active"}).
func handleMongoQuery(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "collection is required", Duration: ms(t0)})
		return
	}

	client, err := getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	filter, err := parseMongoFilter(req.SQL)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: "invalid filter: " + err.Error(), Duration: ms(t0)})
		return
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	limit := int64(req.Limit)
	cursor, err := coll.Find(ctx, filter, options.Find().SetLimit(limit))
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	columns, rows := docsToColumnsRows(docs)
	json.NewEncoder(w).Encode(dbResponse{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Duration: ms(t0),
	})
}

// handleMongoCollections lists collections in the connected database.
func handleMongoCollections(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	names, err := client.Database(req.Connection.Database).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	sort.Strings(names)
	tables := make([]dbTable, len(names))
	for i, name := range names {
		tables[i] = dbTable{Name: name, Type: "collection"}
	}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// handleMongoDescribe samples documents from a collection to infer its field structure.
func handleMongoDescribe(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "collection is required", Duration: ms(t0)})
		return
	}

	client, err := getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	cursor, err := coll.Find(ctx, bson.M{}, options.Find().SetLimit(20))
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	// Collect unique field names across sampled documents.
	seen := make(map[string]bool)
	var fieldNames []string
	for _, doc := range docs {
		for k := range doc {
			if !seen[k] {
				seen[k] = true
				fieldNames = append(fieldNames, k)
			}
		}
	}
	sort.Strings(fieldNames)

	columns := make([]dbColumn, len(fieldNames))
	for i, name := range fieldNames {
		columns[i] = dbColumn{Name: name, Type: inferMongoFieldType(docs, name), Nullable: true}
	}
	json.NewEncoder(w).Encode(dbResponse{Columns: columns, Duration: ms(t0)})
}

// handleMongoDatabases lists all databases on the server.
func handleMongoDatabases(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	client, err := getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	names, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	sort.Strings(names)
	tables := make([]dbTable, len(names))
	for i, name := range names {
		tables[i] = dbTable{Name: name, Type: "database"}
	}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// parseMongoFilter parses a JSON string into a bson.M filter.
// An empty or "{}" string returns an empty filter (match all).
func parseMongoFilter(s string) (bson.M, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" {
		return bson.M{}, nil
	}
	var filter bson.M
	if err := json.Unmarshal([]byte(s), &filter); err != nil {
		return nil, err
	}
	return filter, nil
}

// docsToColumnsRows converts BSON documents to the standard columns/rows format.
// _id is always placed first; remaining fields are sorted alphabetically.
func docsToColumnsRows(docs []bson.M) ([]dbColumn, [][]any) {
	if len(docs) == 0 {
		return []dbColumn{}, [][]any{}
	}

	// Collect all unique keys; _id first.
	seen := make(map[string]bool)
	var keys []string
	for _, doc := range docs {
		if _, ok := doc["_id"]; ok && !seen["_id"] {
			seen["_id"] = true
			keys = append(keys, "_id")
			break
		}
	}

	allKeys := make(map[string]bool)
	for _, doc := range docs {
		for k := range doc {
			allKeys[k] = true
		}
	}
	var rest []string
	for k := range allKeys {
		if k != "_id" {
			rest = append(rest, k)
		}
	}
	sort.Strings(rest)
	keys = append(keys, rest...)

	columns := make([]dbColumn, len(keys))
	for i, k := range keys {
		columns[i] = dbColumn{Name: k, Type: inferMongoFieldType(docs, k)}
	}

	rows := make([][]any, len(docs))
	for i, doc := range docs {
		row := make([]any, len(keys))
		for j, k := range keys {
			row[j] = bsonValueToAny(doc[k])
		}
		rows[i] = row
	}
	return columns, rows
}

// bsonValueToAny converts a BSON value to a JSON-serializable Go value.
func bsonValueToAny(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case primitive.ObjectID:
		return val.Hex()
	case primitive.DateTime:
		return val.Time().UTC().Format(time.RFC3339)
	case primitive.Timestamp:
		return fmt.Sprintf("Timestamp(%d,%d)", val.T, val.I)
	case primitive.Binary:
		return fmt.Sprintf("Binary(%x)", val.Data)
	case primitive.Regex:
		return fmt.Sprintf("/%s/%s", val.Pattern, val.Options)
	case primitive.Decimal128:
		return val.String()
	case bson.M:
		b, _ := json.Marshal(val)
		return string(b)
	case bson.A:
		b, _ := json.Marshal(val)
		return string(b)
	default:
		return v
	}
}

// inferMongoFieldType returns a type label by inspecting the first non-nil value for a field.
func inferMongoFieldType(docs []bson.M, field string) string {
	for _, doc := range docs {
		v, ok := doc[field]
		if !ok || v == nil {
			continue
		}
		switch v.(type) {
		case primitive.ObjectID:
			return "ObjectId"
		case primitive.DateTime:
			return "Date"
		case int32, int64:
			return "int"
		case float64:
			return "double"
		case bool:
			return "bool"
		case string:
			return "string"
		case bson.M, bson.D:
			return "object"
		case bson.A:
			return "array"
		case primitive.Binary:
			return "binary"
		case primitive.Decimal128:
			return "Decimal128"
		default:
			return fmt.Sprintf("%T", v)
		}
	}
	return "mixed"
}
