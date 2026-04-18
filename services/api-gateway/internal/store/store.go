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
	GetWorkflow(ctx context.Context, workflowID string) (Workflow, error)
	ListWorkflows(ctx context.Context, limit int) ([]Workflow, error)
	ListWorkflowEvents(ctx context.Context, workflowID string) ([]eventschema.Event, error)
	ListWorkflowEventsAfter(ctx context.Context, workflowID string, after time.Time, afterEventID string) ([]eventschema.Event, error)
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

func (s *SQLiteStore) GetWorkflow(ctx context.Context, workflowID string) (Workflow, error) {
	const query = `
		SELECT id, correlation_id, prompt, status, created_at, updated_at
		FROM workflows
		WHERE id = ?
	`

	var workflow Workflow
	var createdAt string
	var updatedAt string

	err := s.db.QueryRowContext(ctx, query, workflowID).Scan(
		&workflow.ID,
		&workflow.CorrelationID,
		&workflow.Prompt,
		&workflow.Status,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return Workflow{}, err
		}
		return Workflow{}, fmt.Errorf("query workflow: %w", err)
	}

	workflow.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return Workflow{}, fmt.Errorf("parse workflow created_at: %w", err)
	}
	workflow.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return Workflow{}, fmt.Errorf("parse workflow updated_at: %w", err)
	}

	return workflow, nil
}

func (s *SQLiteStore) ListWorkflows(ctx context.Context, limit int) ([]Workflow, error) {
	if limit <= 0 {
		limit = 20
	}

	const query = `
		SELECT id, correlation_id, prompt, status, created_at, updated_at
		FROM workflows
		ORDER BY updated_at DESC, id DESC
		LIMIT ?
	`

	rows, err := s.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("query workflows: %w", err)
	}
	defer rows.Close()

	workflows := make([]Workflow, 0, limit)
	for rows.Next() {
		var workflow Workflow
		var createdAt string
		var updatedAt string

		if err := rows.Scan(
			&workflow.ID,
			&workflow.CorrelationID,
			&workflow.Prompt,
			&workflow.Status,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}

		workflow.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse workflow created_at: %w", err)
		}
		workflow.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse workflow updated_at: %w", err)
		}

		workflows = append(workflows, workflow)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflows: %w", err)
	}

	return workflows, nil
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

func (s *SQLiteStore) ListWorkflowEventsAfter(ctx context.Context, workflowID string, after time.Time, afterEventID string) ([]eventschema.Event, error) {
	const query = `
		SELECT id, topic, status, source, timestamp, payload
		FROM workflow_events
		WHERE workflow_id = ?
			AND (timestamp > ? OR (timestamp = ? AND id > ?))
		ORDER BY timestamp ASC, id ASC
	`

	rows, err := s.db.QueryContext(ctx, query,
		workflowID,
		after.UTC().Format(time.RFC3339Nano),
		after.UTC().Format(time.RFC3339Nano),
		afterEventID,
	)
	if err != nil {
		return nil, fmt.Errorf("query workflow events after cursor: %w", err)
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
		return nil, fmt.Errorf("iterate workflow events after cursor: %w", err)
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
