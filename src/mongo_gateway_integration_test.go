//go:build integration

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Run with: go test -v -tags integration -run TestMongoAtlas ./...

// loadEnv reads key=value pairs from a .env file into the process environment.
func loadEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if os.Getenv(key) == "" {
			os.Setenv(key, val)
		}
	}
}

func mongoConnectionStringForTest(t *testing.T) string {
	t.Helper()
	loadEnv(".env")
	cs := os.Getenv("MONGO_TEST_CONNECTION_STRING")
	if cs == "" {
		t.Skip("MONGO_TEST_CONNECTION_STRING not set — add it to src/.env or set the env var")
	}
	return cs
}

func mongoPost(t *testing.T, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "mongo")
	rr := httptest.NewRecorder()
	handleMongoGateway(rr, req)
	return rr
}

func decodeMongoResp(t *testing.T, rr *httptest.ResponseRecorder) dbResponse {
	t.Helper()
	var resp dbResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v — body: %s", err, rr.Body.String())
	}
	return resp
}

func TestMongoAtlas_Test(t *testing.T) {
	conn := dbConnection{ConnectionString: mongoConnectionStringForTest(t)}
	rr := mongoPost(t, "test", map[string]any{"connection": conn})
	resp := decodeMongoResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("connection test failed: %s", resp.Error)
	}
	t.Logf("connection OK, duration: %v ms", resp.Duration)
}

func TestMongoAtlas_Databases(t *testing.T) {
	conn := dbConnection{ConnectionString: mongoConnectionStringForTest(t)}
	rr := mongoPost(t, "databases", map[string]any{"connection": conn})
	resp := decodeMongoResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("list databases failed: %s", resp.Error)
	}
	t.Logf("databases (%d):", len(resp.Tables))
	for _, db := range resp.Tables {
		t.Logf("  - %s", db.Name)
	}
}

func TestMongoAtlas_Collections(t *testing.T) {
	conn := dbConnection{ConnectionString: mongoConnectionStringForTest(t), Database: "admin"}
	rr := mongoPost(t, "tables", map[string]any{"connection": conn})
	resp := decodeMongoResp(t, rr)
	if resp.Error != "" {
		t.Fatalf("list collections failed: %s", resp.Error)
	}
	t.Logf("collections in 'admin' (%d):", len(resp.Tables))
	for _, c := range resp.Tables {
		t.Logf("  - %s", c.Name)
	}
}
