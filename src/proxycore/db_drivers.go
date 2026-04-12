package proxycore

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func driverNameFor(driver string) (string, error) {
	switch driver {
	case "postgres", "postgresql":
		return "postgres", nil
	case "mysql":
		return "mysql", nil
	case "sqlite", "sqlite3":
		if !sqliteAvailable() {
			return "", fmt.Errorf("sqlite driver not available — rebuild with -tags sqlite or use CGO_ENABLED=1")
		}
		return "sqlite", nil
	case "clickhouse":
		return "clickhouse", nil
	default:
		return "", fmt.Errorf("unsupported driver %q — supported: postgres, mysql, sqlite, clickhouse", driver)
	}
}

func buildDSN(conn DBConnection) (string, error) {
	switch conn.Driver {
	case "postgres", "postgresql":
		return buildPostgresDSN(conn), nil
	case "mysql":
		return buildMySQLDSN(conn), nil
	case "sqlite", "sqlite3":
		if !sqliteAvailable() {
			return "", fmt.Errorf("sqlite driver not available — rebuild with -tags sqlite or use CGO_ENABLED=1")
		}
		return buildSQLiteDSN(conn), nil
	case "clickhouse":
		return buildClickHouseDSN(conn), nil
	default:
		return "", fmt.Errorf("unsupported driver %q", conn.Driver)
	}
}

func buildPostgresDSN(conn DBConnection) string {
	port := conn.Port
	if port == 0 {
		port = 5432
	}
	ssl := conn.SSL
	if ssl == "" {
		ssl = "disable"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		conn.User, conn.Password, conn.Host, port, conn.Database, ssl)
}

func buildMySQLDSN(conn DBConnection) string {
	port := conn.Port
	if port == 0 {
		port = 3306
	}
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true&allowNativePasswords=true&allowFallbackToPlaintext=true",
		conn.User, conn.Password, conn.Host, port, conn.Database)
}

func buildSQLiteDSN(conn DBConnection) string {
	file := conn.File
	if file == "" {
		file = conn.Database
	}
	if file == "" {
		file = ":memory:"
	}
	return file
}

func buildClickHouseDSN(conn DBConnection) string {
	port := conn.Port
	if port == 0 {
		port = 9000
	}
	useTLS := conn.SSL != "" && conn.SSL != "disable"
	if port == 8123 || port == 8443 {
		scheme := "http"
		if useTLS {
			scheme = "https"
		}
		u := &url.URL{
			Scheme: scheme,
			Host:   fmt.Sprintf("%s:%d", conn.Host, port),
			Path:   "/" + conn.Database,
		}
		if conn.User != "" || conn.Password != "" {
			u.User = url.UserPassword(conn.User, conn.Password)
		}
		q := u.Query()
		// For HTTP(S) protocol, clickhouse-go reliably consumes credentials via
		// query params. Keep userinfo too for compatibility.
		if conn.User != "" {
			q.Set("username", conn.User)
		}
		if conn.Password != "" {
			q.Set("password", conn.Password)
		}
		if useTLS {
			q.Set("secure", "true")
		}
		if len(q) > 0 {
			u.RawQuery = q.Encode()
		}
		return u.String()
	}

	u := &url.URL{
		Scheme: "clickhouse",
		Host:   fmt.Sprintf("%s:%d", conn.Host, port),
		Path:   "/" + conn.Database,
	}
	if conn.User != "" || conn.Password != "" {
		u.User = url.UserPassword(conn.User, conn.Password)
	}
	q := u.Query()
	if useTLS {
		q.Set("secure", "true")
	}
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

func listTables(ctx context.Context, db *sql.DB, conn DBConnection) ([]DBTable, error) {
	switch conn.Driver {
	case "postgres", "postgresql":
		return listTablesPostgres(ctx, db)
	case "mysql":
		return listTablesMySQL(ctx, db, conn.Database)
	case "sqlite", "sqlite3":
		return listTablesSQLite(ctx, db)
	case "clickhouse":
		return listTablesClickHouse(ctx, db)
	default:
		return nil, fmt.Errorf("unsupported driver %q", conn.Driver)
	}
}

func listTablesPostgres(ctx context.Context, db *sql.DB) ([]DBTable, error) {
	const q = `
		SELECT table_schema, table_name, table_type
		FROM information_schema.tables
		WHERE table_schema NOT IN ('pg_catalog','information_schema')
		ORDER BY table_schema, table_name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []DBTable
	for rows.Next() {
		var schema, name, typ string
		if err := rows.Scan(&schema, &name, &typ); err != nil {
			return nil, err
		}
		tableType := "table"
		if typ == "VIEW" {
			tableType = "view"
		}
		tables = append(tables, DBTable{Name: name, Schema: schema, Type: tableType})
	}
	return tables, rows.Err()
}

func listTablesMySQL(ctx context.Context, db *sql.DB, database string) ([]DBTable, error) {
	const q = `
		SELECT table_name, table_type
		FROM information_schema.tables
		WHERE table_schema = DATABASE()
		ORDER BY table_name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []DBTable
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		tableType := "table"
		if typ == "VIEW" {
			tableType = "view"
		}
		tables = append(tables, DBTable{Name: name, Type: tableType})
	}
	return tables, rows.Err()
}

func listTablesSQLite(ctx context.Context, db *sql.DB) ([]DBTable, error) {
	const q = `
		SELECT name, type FROM sqlite_master
		WHERE type IN ('table','view')
		ORDER BY name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []DBTable
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		tables = append(tables, DBTable{Name: name, Type: typ})
	}
	return tables, rows.Err()
}

func listTablesClickHouse(ctx context.Context, db *sql.DB) ([]DBTable, error) {
	const q = `
		SELECT database, name, engine
		FROM system.tables
		WHERE database NOT IN ('system','information_schema','INFORMATION_SCHEMA')
		ORDER BY database, name`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []DBTable
	for rows.Next() {
		var schema, name, engine string
		if err := rows.Scan(&schema, &name, &engine); err != nil {
			return nil, err
		}
		tableType := classifyClickHouseObjectType(engine)
		tables = append(tables, DBTable{Name: name, Schema: schema, Type: tableType})
	}
	return tables, rows.Err()
}

func classifyClickHouseObjectType(engine string) string {
	switch {
	case strings.EqualFold(engine, "RefreshableMaterializedView"):
		return "rmv"
	case strings.EqualFold(engine, "MaterializedView"):
		return "mv"
	case strings.Contains(strings.ToLower(engine), "view"):
		return "view"
	default:
		return "table"
	}
}

func describeTable(ctx context.Context, db *sql.DB, conn DBConnection, table string) ([]DBColumn, error) {
	switch conn.Driver {
	case "postgres", "postgresql":
		return describeTablePostgres(ctx, db, table)
	case "mysql":
		return describeTableMySQL(ctx, db, table)
	case "sqlite", "sqlite3":
		return describeTableSQLite(ctx, db, table)
	case "clickhouse":
		return describeTableClickHouse(ctx, db, table)
	default:
		return nil, fmt.Errorf("unsupported driver %q", conn.Driver)
	}
}

func describeTablePostgres(ctx context.Context, db *sql.DB, table string) ([]DBColumn, error) {
	const q = `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []DBColumn
	for rows.Next() {
		var name, typ, nullable string
		if err := rows.Scan(&name, &typ, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, DBColumn{Name: name, Type: typ, Nullable: nullable == "YES"})
	}
	return cols, rows.Err()
}

func describeTableMySQL(ctx context.Context, db *sql.DB, table string) ([]DBColumn, error) {
	const q = `
		SELECT column_name, column_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = ? AND table_schema = DATABASE()
		ORDER BY ordinal_position`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []DBColumn
	for rows.Next() {
		var name, typ, nullable string
		if err := rows.Scan(&name, &typ, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, DBColumn{Name: name, Type: typ, Nullable: nullable == "YES"})
	}
	return cols, rows.Err()
}

func describeTableSQLite(ctx context.Context, db *sql.DB, table string) ([]DBColumn, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []DBColumn
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull int
		var dflt any
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols = append(cols, DBColumn{Name: name, Type: typ, Nullable: notNull == 0})
	}
	return cols, rows.Err()
}

func describeTableClickHouse(ctx context.Context, db *sql.DB, table string) ([]DBColumn, error) {
	const q = `
		SELECT name, type
		FROM system.columns
		WHERE table = ?
		ORDER BY position`
	rows, err := db.QueryContext(ctx, q, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []DBColumn
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		cols = append(cols, DBColumn{Name: name, Type: typ, Nullable: strings.HasPrefix(typ, "Nullable(")})
	}
	return cols, rows.Err()
}

func listDatabases(ctx context.Context, db *sql.DB, conn DBConnection) ([]string, error) {
	switch conn.Driver {
	case "postgres", "postgresql":
		return listDatabasesPostgres(ctx, db)
	case "mysql":
		return listDatabasesMySQL(ctx, db)
	case "sqlite", "sqlite3":
		file := conn.File
		if file == "" {
			file = conn.Database
		}
		if file == "" {
			file = ":memory:"
		}
		return []string{file}, nil
	case "clickhouse":
		return listDatabasesClickHouse(ctx, db)
	default:
		return nil, fmt.Errorf("unsupported driver %q", conn.Driver)
	}
}

func listDatabasesPostgres(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringColumn(rows)
}

func listDatabasesMySQL(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringColumn(rows)
}

func listDatabasesClickHouse(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "SELECT name FROM system.databases ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanStringColumn(rows)
}

func scanStringColumn(rows *sql.Rows) ([]string, error) {
	var result []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}

var _ = strconv.Itoa // suppress unused import if itoa() in db_gateway.go is used
