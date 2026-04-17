package sqliteutil

import (
	"context"
	"database/sql"
	"fmt"
)

// Configure applies a development-friendly SQLite setup for multiple local
// processes sharing the same database file.
func Configure(ctx context.Context, db *sql.DB) error {
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	for _, statement := range []string{
		"PRAGMA busy_timeout = 5000",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply %q: %w", statement, err)
		}
	}

	return nil
}

func EnsureColumn(ctx context.Context, db *sql.DB, tableName, columnName, definition string) error {
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query table info for %s: %w", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int

		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return fmt.Errorf("scan table info for %s: %w", tableName, err)
		}
		if name == columnName {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table info for %s: %w", tableName, err)
	}

	statement := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", tableName, definition)
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("add %s.%s: %w", tableName, columnName, err)
	}

	return nil
}
