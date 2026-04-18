package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	eventschema "agentmesh/shared/event-schema"
	"agentmesh/shared/event-schema/sqliteutil"
	_ "modernc.org/sqlite"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, event eventschema.Event) error
}

type SQLitePublisher struct {
	db     *sql.DB
	logger *log.Logger
}

func NewSQLitePublisher(dbPath string, logger *log.Logger) (*SQLitePublisher, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if err := sqliteutil.Configure(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite database: %w", err)
	}
	if err := initSchema(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &SQLitePublisher{db: db, logger: logger}, nil
}

func (p *SQLitePublisher) Close() error {
	return p.db.Close()
}

func (p *SQLitePublisher) Publish(ctx context.Context, topic string, event eventschema.Event) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	workflowID, _ := event.Payload["workflow_id"].(string)
	correlationID, _ := event.Payload["correlation_id"].(string)
	eventName, _ := event.Payload["event_name"].(string)

	const query = `
		INSERT INTO workflow_events (
			id,
			workflow_id,
			correlation_id,
			event_name,
			topic,
			status,
			source,
			timestamp,
			payload
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	_, err = p.db.ExecContext(ctx, query,
		event.ID,
		workflowID,
		correlationID,
		eventName,
		topic,
		string(event.Status),
		event.Source,
		event.Timestamp.UTC().Format(time.RFC3339Nano),
		string(payloadJSON),
	)
	if err != nil {
		return fmt.Errorf("insert published event: %w", err)
	}

	p.logger.Printf("published topic=%s event_id=%s correlation_id=%v", topic, event.ID, correlationID)
	return nil
}

func initSchema(ctx context.Context, db *sql.DB) error {
	const schema = `
		CREATE TABLE IF NOT EXISTS workflows (
			id TEXT PRIMARY KEY,
			correlation_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'running',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL DEFAULT '',
			completed_at TEXT,
			failed_at TEXT,
			failure_reason TEXT
		);

		CREATE TABLE IF NOT EXISTS workflow_events (
			id TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			correlation_id TEXT NOT NULL,
			event_name TEXT NOT NULL,
			topic TEXT NOT NULL,
			status TEXT NOT NULL,
			source TEXT NOT NULL,
			timestamp TEXT NOT NULL,
			payload TEXT NOT NULL,
			FOREIGN KEY (workflow_id) REFERENCES workflows(id)
		);
	`

	if _, err := db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize publisher schema: %w", err)
	}

	for _, column := range []struct {
		name       string
		definition string
	}{
		{name: "status", definition: "status TEXT NOT NULL DEFAULT 'running'"},
		{name: "updated_at", definition: "updated_at TEXT NOT NULL DEFAULT ''"},
		{name: "completed_at", definition: "completed_at TEXT"},
		{name: "failed_at", definition: "failed_at TEXT"},
		{name: "failure_reason", definition: "failure_reason TEXT"},
	} {
		if err := sqliteutil.EnsureColumn(ctx, db, "workflows", column.name, column.definition); err != nil {
			return err
		}
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE workflows
		SET updated_at = created_at
		WHERE updated_at = ''
	`); err != nil {
		return fmt.Errorf("backfill workflow timestamps: %w", err)
	}

	return nil
}
