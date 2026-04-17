package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	eventschema "agentmesh/shared/event-schema"
	"agentmesh/shared/event-schema/sqliteutil"
	_ "modernc.org/sqlite"
)

type Workflow struct {
	ID            string    `json:"workflow_id"`
	CorrelationID string    `json:"correlation_id"`
	Prompt        string    `json:"prompt"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Store interface {
	CreateWorkflow(ctx context.Context, workflow Workflow) error
	SaveEvent(ctx context.Context, event eventschema.Event) error
	ListWorkflowEvents(ctx context.Context, workflowID string) ([]eventschema.Event, error)
}

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(dbPath string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if err := sqliteutil.Configure(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite database: %w", err)
	}

	store := &SQLiteStore{db: db}
	if err := store.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) CreateWorkflow(ctx context.Context, workflow Workflow) error {
	const query = `
		INSERT INTO workflows (id, correlation_id, prompt, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`

	_, err := s.db.ExecContext(ctx, query,
		workflow.ID,
		workflow.CorrelationID,
		workflow.Prompt,
		workflow.Status,
		workflow.CreatedAt.UTC().Format(time.RFC3339Nano),
		workflow.UpdatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("insert workflow: %w", err)
	}

	return nil
}

func (s *SQLiteStore) SaveEvent(ctx context.Context, event eventschema.Event) error {
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

	_, err = s.db.ExecContext(ctx, query,
		event.ID,
		workflowID,
		correlationID,
		eventName,
		event.Topic,
		string(event.Status),
		event.Source,
		event.Timestamp.UTC().Format(time.RFC3339Nano),
		string(payloadJSON),
	)
	if err != nil {
		return fmt.Errorf("insert workflow event: %w", err)
	}

	return nil
}

func (s *SQLiteStore) ListWorkflowEvents(ctx context.Context, workflowID string) ([]eventschema.Event, error) {
	const query = `
		SELECT id, topic, status, source, timestamp, payload
		FROM workflow_events
		WHERE workflow_id = ?
		ORDER BY timestamp ASC, id ASC
	`

	rows, err := s.db.QueryContext(ctx, query, workflowID)
	if err != nil {
		return nil, fmt.Errorf("query workflow events: %w", err)
	}
	defer rows.Close()

	events := make([]eventschema.Event, 0)
	for rows.Next() {
		var rawTimestamp string
		var rawPayload string
		var event eventschema.Event
		var status string

		if err := rows.Scan(&event.ID, &event.Topic, &status, &event.Source, &rawTimestamp, &rawPayload); err != nil {
			return nil, fmt.Errorf("scan workflow event: %w", err)
		}

		event.Status = eventschema.EventStatus(status)
		event.Timestamp, err = time.Parse(time.RFC3339Nano, rawTimestamp)
		if err != nil {
			return nil, fmt.Errorf("parse workflow event timestamp: %w", err)
		}
		if err := json.Unmarshal([]byte(rawPayload), &event.Payload); err != nil {
			return nil, fmt.Errorf("unmarshal workflow event payload: %w", err)
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow events: %w", err)
	}

	return events, nil
}

func (s *SQLiteStore) initSchema(ctx context.Context) error {
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

	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize schema: %w", err)
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
		if err := sqliteutil.EnsureColumn(ctx, s.db, "workflows", column.name, column.definition); err != nil {
			return err
		}
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE workflows
		SET updated_at = created_at
		WHERE updated_at = ''
	`); err != nil {
		return fmt.Errorf("backfill workflow timestamps: %w", err)
	}

	return nil
}
