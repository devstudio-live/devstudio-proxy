//go:build integration

package main

import (
	"testing"
)

// Run with: go test -v -tags integration ./...

func TestIntegration_SQLite_Query(t *testing.T) {
	conn := dbConnection{Driver: "sqlite", File: ":memory:"}

	rr := dbPost(t, "test", map[string]any{"connection": conn})
	resp := decodeResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("test connection failed: %s", resp.Error)
	}

	// Create table and insert rows.
	for _, stmt := range []string{
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
		"INSERT INTO users VALUES (1, 'Alice')",
		"INSERT INTO users VALUES (2, 'Bob')",
	} {
		rr := dbPost(t, "query", map[string]any{"connection": conn, "sql": stmt})
		resp := decodeResp(t, rr)
		if resp.Error != "" {
			t.Fatalf("stmt %q failed: %s", stmt, resp.Error)
		}
	}

	// Query the table.
	rr = dbPost(t, "query", map[string]any{
		"connection": conn,
		"sql":        "SELECT * FROM users ORDER BY id",
	})
	resp = decodeResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("query failed: %s", resp.Error)
	}
	if resp.RowCount != 2 {
		t.Errorf("want 2 rows, got %d", resp.RowCount)
	}
	if len(resp.Columns) != 2 {
		t.Errorf("want 2 columns, got %d", len(resp.Columns))
	}

	// List tables.
	rr = dbPost(t, "tables", map[string]any{"connection": conn})
	resp = decodeResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("tables failed: %s", resp.Error)
	}
	if len(resp.Tables) == 0 {
		t.Error("expected at least one table")
	}

	// Describe table.
	rr = dbPost(t, "describe", map[string]any{"connection": conn, "table": "users"})
	resp = decodeResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("describe failed: %s", resp.Error)
	}
	if len(resp.Columns) != 2 {
		t.Errorf("want 2 columns described, got %d", len(resp.Columns))
	}
}
