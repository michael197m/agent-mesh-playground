package bus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	eventschema "agentmesh/shared/event-schema"
	"agentmesh/shared/event-schema/inbox"
	"agentmesh/shared/event-schema/sqliteutil"
	_ "modernc.org/sqlite"
)

type Bus interface {
	Poll(ctx context.Context, topic string, since time.Time, limit int) ([]eventschema.Event, error)
	Publish(ctx context.Context, event eventschema.Event) error
	HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, consumerName, eventID string) error
	Close() error
}

type SQLiteBus struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewSQLiteBus(dbPath string, logger *slog.Logger) (*SQLiteBus, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	if err := sqliteutil.Configure(context.Background(), db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("configure sqlite database: %w", err)
	}

	bus := &SQLiteBus{db: db, logger: logger}
	if err := bus.initSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return bus, nil
}

func (b *SQLiteBus) Close() error {
	return b.db.Close()
}

func (b *SQLiteBus) Poll(ctx context.Context, topic string, since time.Time, limit int) ([]eventschema.Event, error) {
	const query = `
		SELECT id, topic, status, source, timestamp, payload
		FROM workflow_events
		WHERE topic = ? AND timestamp > ?
		ORDER BY timestamp ASC, id ASC
		LIMIT ?
	`

	rows, err := b.db.QueryContext(ctx, query, topic, since.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("query workflow events: %w", err)
	}
	defer rows.Close()

	events := make([]eventschema.Event, 0, limit)
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

func (b *SQLiteBus) Publish(ctx context.Context, event eventschema.Event) error {
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

	_, err = b.db.ExecContext(ctx, query,
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
		return fmt.Errorf("insert published event: %w", err)
	}

	b.logger.Info("published event",
		"event_id", event.ID,
		"topic", event.Topic,
		"event_name", eventName,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
	)

	return nil
}

func (b *SQLiteBus) HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error) {
	return inbox.HasProcessed(ctx, b.db, consumerName, eventID)
}

func (b *SQLiteBus) MarkProcessed(ctx context.Context, consumerName, eventID string) error {
	return inbox.MarkProcessed(ctx, b.db, consumerName, eventID, time.Now().UTC())
}

func (b *SQLiteBus) initSchema(ctx context.Context) error {
	const schema = `
		CREATE TABLE IF NOT EXISTS workflows (
			id TEXT PRIMARY KEY,
			correlation_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			created_at TEXT NOT NULL
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

	if _, err := b.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize bus schema: %w", err)
	}
	if err := inbox.EnsureSchema(ctx, b.db); err != nil {
		return err
	}

	return nil
}

func PayloadKey(payload map[string]any) string {
	if len(payload) == 0 {
		return "{}"
	}

	keys := make([]string, 0, len(payload))
	for key := range payload {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", key, payload[key]))
	}
	return strings.Join(parts, ";")
}
