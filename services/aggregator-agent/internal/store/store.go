package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	eventschema "agentmesh/shared/event-schema"
	"agentmesh/shared/event-schema/inbox"
	"agentmesh/shared/event-schema/sqliteutil"
	_ "modernc.org/sqlite"
)

type WorkflowState struct {
	Prompt                   string
	WorkflowID               string
	CorrelationID            string
	Intent                   string
	RetrievalPayload         map[string]any
	ClassificationPayload    map[string]any
	ResponsePayload          map[string]any
	ResponseRequestedEventID string
	CompletedEventID         string
	FailedEventID            string
	FailureReason            string
	UpdatedAt                time.Time
}

type Store interface {
	HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error)
	RecordResult(ctx context.Context, event eventschema.Event) (WorkflowState, error)
	ListTimedOutWorkflows(ctx context.Context, deadline time.Time, limit int) ([]WorkflowState, error)
	MarkCompleted(ctx context.Context, correlationID, completionEventID string) error
	MarkFailed(ctx context.Context, correlationID, failureEventID, failureReason string) error
	MarkResponseRequested(ctx context.Context, correlationID, responseTaskEventID string) error
	MarkProcessed(ctx context.Context, consumerName, eventID string) error
	Close() error
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

func (s *SQLiteStore) HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error) {
	return inbox.HasProcessed(ctx, s.db, consumerName, eventID)
}

func (s *SQLiteStore) RecordResult(ctx context.Context, event eventschema.Event) (WorkflowState, error) {
	correlationID, _ := event.Payload["correlation_id"].(string)
	workflowID, _ := event.Payload["workflow_id"].(string)
	intent, _ := event.Payload["intent"].(string)
	if correlationID == "" || workflowID == "" {
		return WorkflowState{}, fmt.Errorf("result event missing workflow identifiers")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return WorkflowState{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	if err = s.ensureWorkflowStateRow(ctx, tx, workflowID, correlationID, intent, event.Timestamp); err != nil {
		return WorkflowState{}, err
	}
	if err = s.updateWorkflowState(ctx, tx, correlationID, intent, event); err != nil {
		return WorkflowState{}, err
	}
	if err = s.touchWorkflowProgress(ctx, tx, workflowID, event.Timestamp); err != nil {
		return WorkflowState{}, err
	}

	state, err := s.loadWorkflowStateTx(ctx, tx, correlationID)
	if err != nil {
		return WorkflowState{}, err
	}
	if err = tx.Commit(); err != nil {
		return WorkflowState{}, fmt.Errorf("commit result transaction: %w", err)
	}

	return state, nil
}

func (s *SQLiteStore) MarkCompleted(ctx context.Context, correlationID, completionEventID string) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin completion transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const updateState = `
		UPDATE workflow_state
		SET completed_event_id = ?, updated_at = ?
		WHERE correlation_id = ?
			AND (completed_event_id IS NULL OR completed_event_id = '')
			AND (failed_event_id IS NULL OR failed_event_id = '')
	`
	result, err := tx.ExecContext(ctx, updateState, completionEventID, now.Format(time.RFC3339Nano), correlationID)
	if err != nil {
		return fmt.Errorf("mark workflow completed in state: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read state completion result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("workflow %q was already terminal", correlationID)
	}

	const updateWorkflow = `
		UPDATE workflows
		SET status = 'completed',
			updated_at = ?,
			completed_at = ?,
			failed_at = NULL,
			failure_reason = NULL
		WHERE correlation_id = ?
	`
	if _, err = tx.ExecContext(ctx, updateWorkflow, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), correlationID); err != nil {
		return fmt.Errorf("mark workflow completed in workflows: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit completion transaction: %w", err)
	}

	return nil
}

func (s *SQLiteStore) MarkFailed(ctx context.Context, correlationID, failureEventID, failureReason string) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin failure transaction: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	const ensureState = `
		INSERT INTO workflow_state (workflow_id, correlation_id, intent, updated_at)
		SELECT id, correlation_id, '', ?
		FROM workflows
		WHERE correlation_id = ?
		ON CONFLICT(correlation_id) DO NOTHING
	`
	if _, err = tx.ExecContext(ctx, ensureState, now.Format(time.RFC3339Nano), correlationID); err != nil {
		return fmt.Errorf("ensure workflow state before failure: %w", err)
	}

	const updateState = `
		UPDATE workflow_state
		SET failed_event_id = ?, failure_reason = ?, updated_at = ?
		WHERE correlation_id = ?
			AND (completed_event_id IS NULL OR completed_event_id = '')
			AND (failed_event_id IS NULL OR failed_event_id = '')
	`
	result, err := tx.ExecContext(ctx, updateState, failureEventID, failureReason, now.Format(time.RFC3339Nano), correlationID)
	if err != nil {
		return fmt.Errorf("mark workflow failed in state: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read state failure result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("workflow %q was already terminal", correlationID)
	}

	const updateWorkflow = `
		UPDATE workflows
		SET status = 'failed',
			updated_at = ?,
			failed_at = ?,
			failure_reason = ?
		WHERE correlation_id = ?
	`
	if _, err = tx.ExecContext(ctx, updateWorkflow, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano), failureReason, correlationID); err != nil {
		return fmt.Errorf("mark workflow failed in workflows: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit failure transaction: %w", err)
	}

	return nil
}

func (s *SQLiteStore) MarkProcessed(ctx context.Context, consumerName, eventID string) error {
	return inbox.MarkProcessed(ctx, s.db, consumerName, eventID, time.Now().UTC())
}

func (s *SQLiteStore) MarkResponseRequested(ctx context.Context, correlationID, responseTaskEventID string) error {
	const query = `
		UPDATE workflow_state
		SET response_requested_event_id = ?, updated_at = ?
		WHERE correlation_id = ?
			AND (response_requested_event_id IS NULL OR response_requested_event_id = '')
			AND (completed_event_id IS NULL OR completed_event_id = '')
			AND (failed_event_id IS NULL OR failed_event_id = '')
	`
	result, err := s.db.ExecContext(ctx, query, responseTaskEventID, time.Now().UTC().Format(time.RFC3339Nano), correlationID)
	if err != nil {
		return fmt.Errorf("mark response requested: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read response requested result: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("workflow %q response task was already requested or workflow is terminal", correlationID)
	}
	return nil
}

func (s *SQLiteStore) ListTimedOutWorkflows(ctx context.Context, deadline time.Time, limit int) ([]WorkflowState, error) {
	const query = `
		SELECT
			w.prompt,
			w.id,
			w.correlation_id,
			COALESCE(ws.intent, ''),
			ws.retrieval_payload,
			ws.classification_payload,
			ws.response_payload,
			ws.response_requested_event_id,
			ws.completed_event_id,
			ws.failed_event_id,
			ws.failure_reason,
			w.updated_at
		FROM workflows w
		LEFT JOIN workflow_state ws ON ws.correlation_id = w.correlation_id
		WHERE w.status = 'running'
			AND w.updated_at <= ?
			AND (ws.completed_event_id IS NULL OR ws.completed_event_id = '')
			AND (ws.failed_event_id IS NULL OR ws.failed_event_id = '')
		ORDER BY w.updated_at ASC
		LIMIT ?
	`

	rows, err := s.db.QueryContext(ctx, query, deadline.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("query timed out workflows: %w", err)
	}
	defer rows.Close()

	states := make([]WorkflowState, 0, limit)
	for rows.Next() {
		state, err := scanWorkflowState(rows)
		if err != nil {
			return nil, err
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate timed out workflows: %w", err)
	}

	return states, nil
}

func (s *SQLiteStore) ensureWorkflowStateRow(ctx context.Context, tx *sql.Tx, workflowID, correlationID, intent string, eventTime time.Time) error {
	const query = `
		INSERT INTO workflow_state (workflow_id, correlation_id, intent, updated_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(correlation_id) DO NOTHING
	`
	_, err := tx.ExecContext(ctx, query, workflowID, correlationID, intent, eventTime.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("ensure workflow state row: %w", err)
	}
	return nil
}

func (s *SQLiteStore) updateWorkflowState(ctx context.Context, tx *sql.Tx, correlationID, intent string, event eventschema.Event) error {
	payloadJSON, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("marshal result payload: %w", err)
	}

	fieldName, err := fieldForTopic(event.Topic)
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
		UPDATE workflow_state
		SET %s = ?,
			intent = CASE WHEN ? != '' THEN ? ELSE intent END,
			updated_at = ?
		WHERE correlation_id = ?
	`, fieldName)
	_, err = tx.ExecContext(ctx, query,
		string(payloadJSON),
		intent,
		intent,
		event.Timestamp.UTC().Format(time.RFC3339Nano),
		correlationID,
	)
	if err != nil {
		return fmt.Errorf("update workflow state: %w", err)
	}
	return nil
}

func (s *SQLiteStore) touchWorkflowProgress(ctx context.Context, tx *sql.Tx, workflowID string, eventTime time.Time) error {
	const query = `
		UPDATE workflows
		SET status = 'running',
			updated_at = ?
		WHERE id = ?
	`
	if _, err := tx.ExecContext(ctx, query, eventTime.UTC().Format(time.RFC3339Nano), workflowID); err != nil {
		return fmt.Errorf("touch workflow progress: %w", err)
	}
	return nil
}

func (s *SQLiteStore) loadWorkflowStateTx(ctx context.Context, tx *sql.Tx, correlationID string) (WorkflowState, error) {
	const query = `
		SELECT
			w.prompt,
			ws.workflow_id,
			ws.correlation_id,
			ws.intent,
			ws.retrieval_payload,
			ws.classification_payload,
			ws.response_payload,
			ws.response_requested_event_id,
			ws.completed_event_id,
			ws.failed_event_id,
			ws.failure_reason,
			ws.updated_at
		FROM workflow_state ws
		INNER JOIN workflows w ON w.correlation_id = ws.correlation_id
		WHERE ws.correlation_id = ?
	`

	var state WorkflowState
	row := tx.QueryRowContext(ctx, query, correlationID)
	if err := scanWorkflowStateFromScanner(row, &state); err != nil {
		return WorkflowState{}, fmt.Errorf("load workflow state: %w", err)
	}
	return state, nil
}

func scanWorkflowState(rows *sql.Rows) (WorkflowState, error) {
	var state WorkflowState
	if err := scanWorkflowStateFromScanner(rows, &state); err != nil {
		return WorkflowState{}, err
	}
	return state, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanWorkflowStateFromScanner(scanner rowScanner, state *WorkflowState) error {
	var retrievalPayload sql.NullString
	var classificationPayload sql.NullString
	var responsePayload sql.NullString
	var responseRequestedEventID sql.NullString
	var completedEventID sql.NullString
	var failedEventID sql.NullString
	var failureReason sql.NullString
	var updatedAt string

	if err := scanner.Scan(
		&state.Prompt,
		&state.WorkflowID,
		&state.CorrelationID,
		&state.Intent,
		&retrievalPayload,
		&classificationPayload,
		&responsePayload,
		&responseRequestedEventID,
		&completedEventID,
		&failedEventID,
		&failureReason,
		&updatedAt,
	); err != nil {
		return err
	}

	state.RetrievalPayload, _ = decodeJSONMap(retrievalPayload)
	state.ClassificationPayload, _ = decodeJSONMap(classificationPayload)
	state.ResponsePayload, _ = decodeJSONMap(responsePayload)
	state.ResponseRequestedEventID = responseRequestedEventID.String
	state.CompletedEventID = completedEventID.String
	state.FailedEventID = failedEventID.String
	state.FailureReason = failureReason.String
	state.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
	return nil
}

func decodeJSONMap(raw sql.NullString) (map[string]any, error) {
	if !raw.Valid || raw.String == "" {
		return nil, nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw.String), &payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func fieldForTopic(topic string) (string, error) {
	switch topic {
	case "mesh.result.retrieval":
		return "retrieval_payload", nil
	case "mesh.result.classification":
		return "classification_payload", nil
	case "mesh.result.response":
		return "response_payload", nil
	default:
		return "", fmt.Errorf("unsupported result topic %q", topic)
	}
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

		CREATE TABLE IF NOT EXISTS workflow_state (
			workflow_id TEXT NOT NULL,
			correlation_id TEXT PRIMARY KEY,
			intent TEXT NOT NULL DEFAULT '',
			retrieval_payload TEXT,
			classification_payload TEXT,
			response_payload TEXT,
			response_requested_event_id TEXT,
			completed_event_id TEXT,
			failed_event_id TEXT,
			failure_reason TEXT,
			updated_at TEXT NOT NULL
		)
	`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize aggregator schema: %w", err)
	}
	for _, column := range []struct {
		table      string
		name       string
		definition string
	}{
		{table: "workflows", name: "status", definition: "status TEXT NOT NULL DEFAULT 'running'"},
		{table: "workflows", name: "updated_at", definition: "updated_at TEXT NOT NULL DEFAULT ''"},
		{table: "workflows", name: "completed_at", definition: "completed_at TEXT"},
		{table: "workflows", name: "failed_at", definition: "failed_at TEXT"},
		{table: "workflows", name: "failure_reason", definition: "failure_reason TEXT"},
		{table: "workflow_state", name: "completed_event_id", definition: "completed_event_id TEXT"},
		{table: "workflow_state", name: "response_requested_event_id", definition: "response_requested_event_id TEXT"},
		{table: "workflow_state", name: "failed_event_id", definition: "failed_event_id TEXT"},
		{table: "workflow_state", name: "failure_reason", definition: "failure_reason TEXT"},
	} {
		if err := sqliteutil.EnsureColumn(ctx, s.db, column.table, column.name, column.definition); err != nil {
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
	if err := inbox.EnsureSchema(ctx, s.db); err != nil {
		return err
	}
	return nil
}
