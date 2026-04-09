package proxycore

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// DBRequest is the unified request body for all DB gateway endpoints.
type DBRequest struct {
	Connection DBConnection `json:"connection"`
	SQL        string       `json:"sql"`
	Table      string       `json:"table"`
	Limit      int          `json:"limit"`
	Filter     string       `json:"filter"` // JSON filter doc (MongoDB write operations)
	Doc        string       `json:"doc"`    // JSON document body (MongoDB insert)
}

// DBResponse is the unified response body for all DB gateway endpoints.
type DBResponse struct {
	Columns  []DBColumn `json:"columns"`
	Rows     [][]any    `json:"rows"`
	RowCount int        `json:"rowCount"`
	Affected int64      `json:"affected"`
	Duration float64    `json:"durationMs"`
	Tables   []DBTable  `json:"tables"`
	Error    string     `json:"error,omitempty"`
}

// DBColumn describes a result-set column.
type DBColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// DBTable describes a table or view.
type DBTable struct {
	Name   string `json:"name"`
	Schema string `json:"schema,omitempty"`
	Type   string `json:"type"`
}

const (
	defaultRowLimit = 1000
	maxRowLimit     = 10000
	queryTimeout    = 30 * time.Second
)

func (s *Server) handleDBGateway(w http.ResponseWriter, r *http.Request) {
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
		s.handleDBTest(w, r, req)
	case "query":
		s.handleDBQuery(w, r, req)
	case "tables":
		s.handleDBTables(w, r, req)
	case "describe":
		s.handleDBDescribe(w, r, req)
	case "databases":
		s.handleDBDatabases(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(DBResponse{Error: "unknown gateway endpoint: " + path})
	}
}

func (s *Server) handleDBTest(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	db, err := s.getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Duration: ms(t0)})
}

func (s *Server) handleDBQuery(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.SQL == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "sql is required", Duration: ms(t0)})
		return
	}

	db, err := s.getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

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
			json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
			return
		}
		affected, _ := result.RowsAffected()
		json.NewEncoder(w).Encode(DBResponse{Affected: affected, Duration: ms(t0), Columns: []DBColumn{}, Rows: [][]any{}})
		return
	}

	sql := req.SQL
	if !strings.Contains(upperSQL, "LIMIT") {
		sql = addLimit(sql, req.Limit, req.Connection.Driver)
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	colTypes, _ := rows.ColumnTypes()

	columns := make([]DBColumn, len(cols))
	for i, name := range cols {
		col := DBColumn{Name: name}
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
			json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
			return
		}
		for i, v := range vals {
			if b, ok := v.([]byte); ok {
				vals[i] = string(b)
			}
		}
		resultRows = append(resultRows, vals)
	}
	if err := rows.Err(); err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	if resultRows == nil {
		resultRows = [][]any{}
	}

	json.NewEncoder(w).Encode(DBResponse{
		Columns:  columns,
		Rows:     resultRows,
		RowCount: len(resultRows),
		Duration: ms(t0),
	})
}

func (s *Server) handleDBTables(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	db, err := s.getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	tables, err := listTables(ctx, db, req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Tables: tables, Duration: ms(t0)})
}

func (s *Server) handleDBDescribe(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	if req.Table == "" {
		json.NewEncoder(w).Encode(DBResponse{Error: "table is required", Duration: ms(t0)})
		return
	}

	db, err := s.getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	columns, err := describeTable(ctx, db, req.Connection, req.Table)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(DBResponse{Columns: columns, Duration: ms(t0)})
}

func (s *Server) handleDBDatabases(w http.ResponseWriter, r *http.Request, req DBRequest) {
	t0 := time.Now()
	db, err := s.getPooledDB(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
	defer cancel()

	dbs, err := listDatabases(ctx, db, req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(DBResponse{Error: err.Error(), Duration: ms(t0)})
		return
	}

	tables := make([]DBTable, len(dbs))
	for i, name := range dbs {
		tables[i] = DBTable{Name: name, Type: "database"}
	}
	json.NewEncoder(w).Encode(DBResponse{Tables: tables, Duration: ms(t0)})
}

func ms(t0 time.Time) float64 {
	return float64(time.Since(t0).Microseconds()) / 1000.0
}

func addLimit(sql string, limit int, driver string) string {
	return strings.TrimRight(sql, " \t\n\r;") + " LIMIT " + itoa(limit)
}

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
