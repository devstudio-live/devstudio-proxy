package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// dbRequest is the unified request body for all DB gateway endpoints.
type dbRequest struct {
	Connection dbConnection `json:"connection"`
	SQL        string       `json:"sql"`
	Table      string       `json:"table"`
	Limit      int          `json:"limit"`
}

// dbResponse is the unified response body for all DB gateway endpoints.
type dbResponse struct {
	Columns  []dbColumn `json:"columns"`
	Rows     [][]any    `json:"rows"`
	RowCount int        `json:"rowCount"`
	Affected int64      `json:"affected"`
	Duration float64    `json:"durationMs"`
	Tables   []dbTable  `json:"tables"`
	Error    string     `json:"error,omitempty"`
}

// dbColumn describes a result-set column.
type dbColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// dbTable describes a table or view.
type dbTable struct {
	Name   string `json:"name"`
	Schema string `json:"schema,omitempty"`
	Type   string `json:"type"` // "table" | "view"
}

const (
	defaultRowLimit = 1000
	maxRowLimit     = 10000
	queryTimeout    = 30 * time.Second
)

// handleDBGateway is the entry-point for all SQL gateway requests.
// It strips the gateway routing headers (defense-in-depth) and dispatches
// to the appropriate sub-handler based on r.URL.Path.
func handleDBGateway(w http.ResponseWriter, r *http.Request) {
	// Strip gateway routing headers so they are never forwarded upstream.
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	// Only POST is accepted.
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

	// Apply row limit defaults and caps.
	if req.Limit <= 0 {
		req.Limit = defaultRowLimit
	}
	if req.Limit > maxRowLimit {
		req.Limit = maxRowLimit
	}

	// Dispatch by path (path is flexible since routing is header-based).
	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "test":
		handleDBTest(w, r, req)
	case "query":
		handleDBQuery(w, r, req)
	case "tables":
		handleDBTables(w, r, req)
	case "describe":
		handleDBDescribe(w, r, req)
	case "databases":
		handleDBDatabases(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(dbResponse{Error: "unknown gateway endpoint: " + path})
	}
}

// handleDBTest tests the connection and returns ok or an error message.
func handleDBTest(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	db, err := getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(dbResponse{Duration: ms(t0)})
}

// handleDBQuery executes arbitrary SQL and returns columns + rows.
func handleDBQuery(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.SQL == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "sql is required", Duration: ms(t0)})
		return
	}

	db, err := getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	// Decide: query vs. exec based on whether it looks like a SELECT-like statement.
	upperSQL := strings.TrimSpace(strings.ToUpper(req.SQL))
	isQuery := strings.HasPrefix(upperSQL, "SELECT") ||
		strings.HasPrefix(upperSQL, "WITH") ||
		strings.HasPrefix(upperSQL, "SHOW") ||
		strings.HasPrefix(upperSQL, "EXPLAIN") ||
		strings.HasPrefix(upperSQL, "DESCRIBE") ||
		strings.HasPrefix(upperSQL, "PRAGMA")

	if !isQuery {
		result, err := db.ExecContext(ctx, req.SQL)
		if err != nil {
			json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
			return
		}
		affected, _ := result.RowsAffected()
		json.NewEncoder(w).Encode(dbResponse{Affected: affected, Duration: ms(t0), Columns: []dbColumn{}, Rows: [][]any{}})
		return
	}

	// Append LIMIT if the statement doesn't already have one.
	sql := req.SQL
	if !strings.Contains(upperSQL, "LIMIT") {
		sql = addLimit(sql, req.Limit, req.Connection.Driver)
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	colTypes, _ := rows.ColumnTypes()

	columns := make([]dbColumn, len(cols))
	for i, name := range cols {
		col := dbColumn{Name: name}
		if i < len(colTypes) {
			col.Type = colTypes[i].DatabaseTypeName()
			if nullable, ok := colTypes[i].Nullable(); ok {
				col.Nullable = nullable
			}
		}
		columns[i] = col
	}

	var resultRows [][]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
			return
		}
		// Convert []byte to string for JSON-friendliness.
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		resultRows = append(resultRows, vals)
	}
	if err := rows.Err(); err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	if resultRows == nil {
		resultRows = [][]any{}
	}

	json.NewEncoder(w).Encode(dbResponse{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Duration: ms(t0),
	})
}

// handleDBTables lists tables and views in the connected database.
func handleDBTables(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	db, err := getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	tables, err := listTables(ctx, db, req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// handleDBDescribe returns column metadata for a specific table.
func handleDBDescribe(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(dbResponse{Error: "table is required", Duration: ms(t0)})
		return
	}

	db, err := getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	columns, err := describeTable(ctx, db, req.Connection, req.Table)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(dbResponse{Columns: columns, Duration: ms(t0)})
}

// handleDBDatabases lists available databases/schemas on the server.
func handleDBDatabases(w http.ResponseWriter, r *http.Request, req dbRequest) {
	t0 := time.Now()
	db, err := getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	dbs, err := listDatabases(ctx, db, req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(dbResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	// Return as tables with type "database" for a uniform response.
	tables := make([]dbTable, len(dbs))
	for i, name := range dbs {
		tables[i] = dbTable{Name: name, Type: "database"}
	}
	json.NewEncoder(w).Encode(dbResponse{Tables: tables, Duration: ms(t0)})
}

// ms returns elapsed milliseconds since t0 as a float64.
func ms(t0 time.Time) float64 {
	return float64(time.Since(t0).Microseconds()) / 1000.0
}

// addLimit appends a LIMIT clause to a SQL statement that doesn't have one.
func addLimit(sql string, limit int, driver string) string {
	// SQLite and PostgreSQL/MySQL all support LIMIT n.
	return strings.TrimRight(sql, " \t\n\r;") + " LIMIT " + itoa(limit)
}

// itoa converts an int to its decimal string representation without importing strconv.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
