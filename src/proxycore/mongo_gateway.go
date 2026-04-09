package proxycore

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
func (s *Server) handleMongoGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(DBResponse{Error: "only POST is accepted"})
		return
	}

	var req DBRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid JSON: " + err.Error()})
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
		s.handleMongoTest(w, r, req)
	case "query":
		s.handleMongoQuery(w, r, req)
	case "tables":
		s.handleMongoCollections(w, r, req)
	case "describe":
		s.handleMongoDescribe(w, r, req)
	case "databases":
		s.handleMongoDatabases(w, r, req)
	case "admin":
		s.handleMongoAdmin(w, r, req)
	case "aggregate":
		s.handleMongoAggregate(w, r, req)
	case "insert":
		s.handleMongoInsert(w, r, req)
	case "update":
		s.handleMongoUpdate(w, r, req)
	case "delete":
		s.handleMongoDelete(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(DBResponse{Error: "unknown gateway endpoint: " + path})
	}
}

func (s *Server) handleMongoTest(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		s.evictMongoClient(req.Connection)
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Duration: ms(t0)})
}

// handleMongoQuery executes a find against a collection.
// req.Table = collection name, req.SQL = JSON filter (e.g. {"status":"active"}).
func (s *Server) handleMongoQuery(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "collection is required", Duration: ms(t0)})
		return
	}

	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	filter, err := parseMongoFilter(req.SQL)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid filter: " + err.Error(), Duration: ms(t0)})
		return
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	limit := int64(req.Limit)
	cursor, err := coll.Find(ctx, filter, options.Find().SetLimit(limit))
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	columns, rows := docsToColumnsRows(docs)
	json.NewEncoder(w).Encode(DBResponse{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Duration: ms(t0),
	})
}

// handleMongoCollections lists collections in the connected database.
func (s *Server) handleMongoCollections(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	names, err := client.Database(req.Connection.Database).ListCollectionNames(ctx, bson.M{})
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	sort.Strings(names)
	tables := make([]DBTable, len(names))
	for i, name := range names {
		tables[i] = DBTable{Name: name, Type: "collection"}
	}
	json.NewEncoder(w).Encode(DBResponse{Tables: tables, Duration: ms(t0)})
}

// handleMongoDescribe samples documents from a collection to infer its field structure.
func (s *Server) handleMongoDescribe(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "collection is required", Duration: ms(t0)})
		return
	}

	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	cursor, err := coll.Find(ctx, bson.M{}, options.Find().SetLimit(20))
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
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

	columns := make([]DBColumn, len(fieldNames))
	for i, name := range fieldNames {
		columns[i] = DBColumn{Name: name, Type: inferMongoFieldType(docs, name), Nullable: true}
	}
	json.NewEncoder(w).Encode(DBResponse{Columns: columns, Duration: ms(t0)})
}

// handleMongoDatabases lists all databases on the server.
func (s *Server) handleMongoDatabases(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	names, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	sort.Strings(names)
	tables := make([]DBTable, len(names))
	for i, name := range names {
		tables[i] = DBTable{Name: name, Type: "database"}
	}
	json.NewEncoder(w).Encode(DBResponse{Tables: tables, Duration: ms(t0)})
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
func docsToColumnsRows(docs []bson.M) ([]DBColumn, [][]any) {
	if len(docs) == 0 {
		return []DBColumn{}, [][]any{}
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

	columns := make([]DBColumn, len(keys))
	for i, k := range keys {
		columns[i] = DBColumn{Name: k, Type: inferMongoFieldType(docs, k)}
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

// handleMongoAggregate executes an aggregation pipeline against a collection.
// req.Table = collection name, req.SQL = JSON array of pipeline stages.
// A $limit stage is appended automatically if none is present and req.Limit > 0.
func (s *Server) handleMongoAggregate(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "collection name is required (table field)", Duration: ms(t0)})
		return
	}
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	pipelineStr := strings.TrimSpace(req.SQL)
	if pipelineStr == "" {
		pipelineStr = "[]"
	}
	var pipeline bson.A
	if err := bson.UnmarshalExtJSON([]byte(pipelineStr), true, &pipeline); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid pipeline JSON: " + err.Error(), Duration: ms(t0)})
		return
	}

	// Append $limit if no explicit $limit stage exists and req.Limit > 0
	if req.Limit > 0 {
		hasLimit := false
		for _, stage := range pipeline {
			if m, ok := stage.(bson.D); ok {
				for _, e := range m {
					if e.Key == "$limit" {
						hasLimit = true
						break
					}
				}
			} else if m, ok := stage.(bson.M); ok {
				if _, ok := m["$limit"]; ok {
					hasLimit = true
				}
			}
			if hasLimit {
				break
			}
		}
		if !hasLimit {
			pipeline = append(pipeline, bson.D{{Key: "$limit", Value: int64(req.Limit)}})
		}
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	opts := options.Aggregate().SetAllowDiskUse(true)
	cursor, err := coll.Aggregate(ctx, pipeline, opts)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer cursor.Close(ctx)

	var docs []bson.M
	if err := cursor.All(ctx, &docs); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	columns, rows := docsToColumnsRows(docs)
	json.NewEncoder(w).Encode(DBResponse{
		Columns:  columns,
		Rows:     rows,
		RowCount: len(rows),
		Duration: ms(t0),
	})
}

// handleMongoAdmin runs an arbitrary MongoDB command against a specified database
// (req.Table, defaulting to "admin"). The command document is given as JSON in req.SQL.
// The result is returned as a JSON string in rows[0][0] so the client can parse it.
func (s *Server) handleMongoAdmin(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	cmdStr := strings.TrimSpace(req.SQL)
	if cmdStr == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "sql field must contain the command document (JSON)", Duration: ms(t0)})
		return
	}
	var cmdDoc bson.D
	if err := bson.UnmarshalExtJSON([]byte(cmdStr), true, &cmdDoc); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid command JSON: " + err.Error(), Duration: ms(t0)})
		return
	}

	dbName := req.Table
	if dbName == "" {
		dbName = "admin"
	}

	var result bson.M
	if err := client.Database(dbName).RunCommand(ctx, cmdDoc).Decode(&result); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	safe := deepBsonToAny(result)
	b, err := json.Marshal(safe)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "marshal error: " + err.Error(), Duration: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(DBResponse{
		Columns:  []DBColumn{{Name: "result", Type: "json"}},
		Rows:     [][]any{{string(b)}},
		RowCount: 1,
		Duration: ms(t0),
	})
}

// deepBsonToAny recursively converts bson.M / bson.D / bson.A and primitive types
// to JSON-safe Go values, so they can be marshalled with encoding/json.
func deepBsonToAny(v any) any {
	switch val := v.(type) {
	case bson.M:
		m := make(map[string]any, len(val))
		for k, v2 := range val {
			m[k] = deepBsonToAny(v2)
		}
		return m
	case bson.D:
		m := make(map[string]any, len(val))
		for _, e := range val {
			m[e.Key] = deepBsonToAny(e.Value)
		}
		return m
	case bson.A:
		arr := make([]any, len(val))
		for i, v2 := range val {
			arr[i] = deepBsonToAny(v2)
		}
		return arr
	default:
		return bsonValueToAny(v)
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

// handleMongoInsert inserts a single document into a collection.
// req.Table = collection name, req.Doc = JSON document to insert.
func (s *Server) handleMongoInsert(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "table (collection) is required", Duration: ms(t0)})
		return
	}
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	docStr := strings.TrimSpace(req.Doc)
	if docStr == "" {
		docStr = "{}"
	}
	var doc bson.M
	if err := bson.UnmarshalExtJSON([]byte(docStr), true, &doc); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid doc JSON: " + err.Error(), Duration: ms(t0)})
		return
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	res, err := coll.InsertOne(ctx, doc)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	_ = res
	json.NewEncoder(w).Encode(DBResponse{Affected: 1, Duration: ms(t0)})
}

// handleMongoUpdate updates the first document matching the filter.
// req.Table = collection, req.Filter = JSON filter, req.SQL = JSON update doc (e.g. {"$set":{...}}).
func (s *Server) handleMongoUpdate(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "table (collection) is required", Duration: ms(t0)})
		return
	}
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	filter, err := parseMongoFilter(req.Filter)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid filter: " + err.Error(), Duration: ms(t0)})
		return
	}

	updateStr := strings.TrimSpace(req.SQL)
	if updateStr == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "sql field must contain the update document", Duration: ms(t0)})
		return
	}
	var update bson.M
	if err := bson.UnmarshalExtJSON([]byte(updateStr), true, &update); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid update JSON: " + err.Error(), Duration: ms(t0)})
		return
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	res, err := coll.UpdateOne(ctx, filter, update)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Affected: res.ModifiedCount, Duration: ms(t0)})
}

// handleMongoDelete deletes the first document matching the filter.
// req.Table = collection, req.Filter = JSON filter.
func (s *Server) handleMongoDelete(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "table (collection) is required", Duration: ms(t0)})
		return
	}
	client, err := s.getPooledMongoClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	filter, err := parseMongoFilter(req.Filter)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: "invalid filter: " + err.Error(), Duration: ms(t0)})
		return
	}

	coll := client.Database(req.Connection.Database).Collection(req.Table)
	res, err := coll.DeleteOne(ctx, filter)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Affected: res.DeletedCount, Duration: ms(t0)})
}
