package inbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func EnsureSchema(ctx context.Context, execer Execer) error {
	const query = `
		CREATE TABLE IF NOT EXISTS inbox (
			event_id TEXT NOT NULL,
			consumer_name TEXT NOT NULL,
			processed_at TEXT NOT NULL,
			PRIMARY KEY (event_id, consumer_name)
		)
	`
	if _, err := execer.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("initialize inbox schema: %w", err)
	}
	return nil
}

func HasProcessed(ctx context.Context, queryer Queryer, consumerName, eventID string) (bool, error) {
	const query = `
		SELECT 1
		FROM inbox
		WHERE event_id = ? AND consumer_name = ?
		LIMIT 1
	`
	var exists int
	err := queryer.QueryRowContext(ctx, query, eventID, consumerName).Scan(&exists)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query inbox: %w", err)
	}
	return true, nil
}

func MarkProcessed(ctx context.Context, execer Execer, consumerName, eventID string, processedAt time.Time) error {
	const query = `
		INSERT OR IGNORE INTO inbox (event_id, consumer_name, processed_at)
		VALUES (?, ?, ?)
	`
	if _, err := execer.ExecContext(ctx, query, eventID, consumerName, processedAt.UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("insert inbox row: %w", err)
	}
	return nil
}
