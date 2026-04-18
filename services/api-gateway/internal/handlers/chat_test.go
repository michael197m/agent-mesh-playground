package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agentmesh/services/api-gateway/internal/bus"
	"agentmesh/services/api-gateway/internal/store"
	eventschema "agentmesh/shared/event-schema"
)

func TestWorkflowIDFromPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		suffix     string
		wantID     string
		wantParsed bool
	}{
		{
			name:       "valid path",
			path:       "/api/workflows/workflow-123/events",
			suffix:     "/events",
			wantID:     "workflow-123",
			wantParsed: true,
		},
		{
			name:       "missing workflow id",
			path:       "/api/workflows//events",
			suffix:     "/events",
			wantParsed: false,
		},
		{
			name:       "nested segment",
			path:       "/api/workflows/workflow-123/extra/events",
			suffix:     "/events",
			wantParsed: false,
		},
		{
			name:       "wrong suffix",
			path:       "/api/workflows/workflow-123",
			suffix:     "/events",
			wantParsed: false,
		},
		{
			name:       "approval path",
			path:       "/api/workflows/workflow-123/approval",
			suffix:     "/approval",
			wantID:     "workflow-123",
			wantParsed: true,
		},
		{
			name:       "stream path",
			path:       "/api/workflows/workflow-123/stream",
			suffix:     "/stream",
			wantID:     "workflow-123",
			wantParsed: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotParsed := workflowIDFromPath(tt.path, tt.suffix)
			if gotParsed != tt.wantParsed {
				t.Fatalf("parsed = %v, want %v", gotParsed, tt.wantParsed)
			}
			if gotID != tt.wantID {
				t.Fatalf("workflowID = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}

func TestWorkflowRootIDFromPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantID     string
		wantParsed bool
	}{
		{name: "valid root path", path: "/api/workflows/workflow-123", wantID: "workflow-123", wantParsed: true},
		{name: "trim trailing slash", path: "/api/workflows/workflow-123/", wantID: "workflow-123", wantParsed: true},
		{name: "missing id", path: "/api/workflows/", wantParsed: false},
		{name: "nested path", path: "/api/workflows/workflow-123/events", wantParsed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotParsed := workflowRootIDFromPath(tt.path)
			if gotParsed != tt.wantParsed {
				t.Fatalf("parsed = %v, want %v", gotParsed, tt.wantParsed)
			}
			if gotID != tt.wantID {
				t.Fatalf("workflowID = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}

func TestIsWorkflowCollectionPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/api/workflows", want: true},
		{path: "/api/workflows/", want: true},
		{path: "/api/workflows/workflow-123", want: false},
		{path: "/api/workflows/workflow-123/events", want: false},
	}

	for _, tt := range tests {
		if got := isWorkflowCollectionPath(tt.path); got != tt.want {
			t.Fatalf("isWorkflowCollectionPath(%q) = %v, want %v", tt.path, got, tt.want)
		}
	}
}

func TestChatHandlerPublishesRequestEventToSQLite(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "mesh.db")

	workflowStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore() error = %v", err)
	}
	defer func() {
		if err := workflowStore.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	}()

	publisher, err := bus.NewSQLitePublisher(dbPath, log.New(os.Stdout, "", 0))
	if err != nil {
		t.Fatalf("NewSQLitePublisher() error = %v", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			t.Fatalf("publisher Close() error = %v", err)
		}
	}()

	handler := NewChatHandler(publisher, workflowStore)

	request := httptest.NewRequest(http.MethodPost, "/api/chat", strings.NewReader(`{"prompt":"investigate packet loss"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusAccepted)
	}

	var response ChatResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.WorkflowID == "" {
		t.Fatal("workflow_id is empty")
	}
	if response.CorrelationID == "" {
		t.Fatal("correlation_id is empty")
	}

	events, err := workflowStore.ListWorkflowEvents(request.Context(), response.WorkflowID)
	if err != nil {
		t.Fatalf("ListWorkflowEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("len(events) = %d, want 1", len(events))
	}
	if events[0].Topic != requestTopic {
		t.Fatalf("events[0].Topic = %q, want %q", events[0].Topic, requestTopic)
	}
	if got, _ := events[0].Payload["event_name"].(string); got != "request.received" {
		t.Fatalf("event_name = %q, want request.received", got)
	}
	if got, _ := events[0].Payload["workflow_id"].(string); got != response.WorkflowID {
		t.Fatalf("workflow_id = %q, want %q", got, response.WorkflowID)
	}
	if got, _ := events[0].Payload["correlation_id"].(string); got != response.CorrelationID {
		t.Fatalf("correlation_id = %q, want %q", got, response.CorrelationID)
	}
}

func TestBuildWorkflowSummary(t *testing.T) {
	workflow := store.Workflow{
		ID:            "workflow-123",
		CorrelationID: "corr-123",
		Prompt:        "Investigate packet loss",
		Status:        "running",
		CreatedAt:     time.Date(2026, 4, 18, 0, 0, 0, 0, time.UTC),
		UpdatedAt:     time.Date(2026, 4, 18, 0, 1, 0, 0, time.UTC),
	}
	events := []eventschema.Event{
		{
			ID:        "plan",
			Topic:     "mesh.plan",
			Status:    eventschema.EventStatusSucceeded,
			Source:    "planner-agent",
			Timestamp: workflow.CreatedAt,
			Payload: map[string]any{
				"intent": "incident triage",
			},
		},
		{
			ID:        "approval-required",
			Topic:     "mesh.approval.required",
			Status:    eventschema.EventStatusPending,
			Source:    "planner-agent",
			Timestamp: workflow.CreatedAt.Add(5 * time.Second),
			Payload: map[string]any{
				"reason": "approval required for rollback",
			},
		},
		{
			ID:        "approval-received",
			Topic:     "mesh.approval.received",
			Status:    eventschema.EventStatusSucceeded,
			Source:    "api-gateway",
			Timestamp: workflow.CreatedAt.Add(10 * time.Second),
			Payload: map[string]any{
				"decision": "approved",
				"comment":  "validated by operator",
			},
		},
		{
			ID:        "completed",
			Topic:     "mesh.workflow.completed",
			Status:    eventschema.EventStatusSucceeded,
			Source:    "aggregator-agent",
			Timestamp: workflow.CreatedAt.Add(20 * time.Second),
			Payload: map[string]any{
				"intent":                "incident triage",
				"approval_decision":     "approved",
				"approval_comment":      "validated by operator",
				"retrieval_result":      map[string]any{"summary": "retrieved context"},
				"classification_result": map[string]any{"classification_result": "severity=high"},
				"response_result":       map[string]any{"response": "rollback recommendation ready"},
			},
		},
	}

	summary := buildWorkflowSummary(workflow, events)
	if summary.WorkflowID != workflow.ID {
		t.Fatalf("WorkflowID = %q, want %q", summary.WorkflowID, workflow.ID)
	}
	if summary.Status != "completed" {
		t.Fatalf("Status = %q, want completed", summary.Status)
	}
	if summary.EventCount != len(events) {
		t.Fatalf("EventCount = %d, want %d", summary.EventCount, len(events))
	}
	if summary.Intent != "incident triage" {
		t.Fatalf("Intent = %q, want incident triage", summary.Intent)
	}
	if summary.ApprovalStatus != "received" {
		t.Fatalf("ApprovalStatus = %q, want received", summary.ApprovalStatus)
	}
	if summary.ApprovalDecision != "approved" {
		t.Fatalf("ApprovalDecision = %q, want approved", summary.ApprovalDecision)
	}
	if summary.ApprovalComment != "validated by operator" {
		t.Fatalf("ApprovalComment = %q, want validated by operator", summary.ApprovalComment)
	}
	if got, _ := summary.ResponseResult["response"].(string); got != "rollback recommendation ready" {
		t.Fatalf("response = %q, want rollback recommendation ready", got)
	}
	if summary.LatestEvent == nil || summary.LatestEvent.ID != "completed" {
		t.Fatalf("LatestEvent = %+v, want completed event", summary.LatestEvent)
	}
}
