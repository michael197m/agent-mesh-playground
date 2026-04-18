package handlers

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"agentmesh/services/api-gateway/internal/bus"
	"agentmesh/services/api-gateway/internal/store"
	eventschema "agentmesh/shared/event-schema"
)

const requestTopic = "mesh.request"

type ChatHandler struct {
	publisher bus.Publisher
	store     store.Store
	now       func() time.Time
}

type ChatRequest struct {
	Prompt string `json:"prompt"`
}

type ChatResponse struct {
	WorkflowID    string `json:"workflow_id"`
	CorrelationID string `json:"correlation_id"`
}

type WorkflowEventsResponse struct {
	Events []eventschema.Event `json:"events"`
}

type WorkflowHistoryResponse struct {
	Workflows []WorkflowSummary `json:"workflows"`
}

type WorkflowSummary struct {
	WorkflowID           string             `json:"workflow_id"`
	CorrelationID        string             `json:"correlation_id"`
	Prompt               string             `json:"prompt"`
	Status               string             `json:"status"`
	CreatedAt            time.Time          `json:"created_at"`
	UpdatedAt            time.Time          `json:"updated_at"`
	EventCount           int                `json:"event_count"`
	Intent               string             `json:"intent,omitempty"`
	ApprovalStatus       string             `json:"approval_status,omitempty"`
	ApprovalDecision     string             `json:"approval_decision,omitempty"`
	ApprovalComment      string             `json:"approval_comment,omitempty"`
	FailureReason        string             `json:"failure_reason,omitempty"`
	RetrievalResult      map[string]any     `json:"retrieval_result,omitempty"`
	ClassificationResult map[string]any     `json:"classification_result,omitempty"`
	ResponseResult       map[string]any     `json:"response_result,omitempty"`
	LatestEvent          *eventschema.Event `json:"latest_event,omitempty"`
}

type ApprovalRequest struct {
	Decision string `json:"decision"`
	Comment  string `json:"comment"`
}

func NewChatHandler(publisher bus.Publisher, workflowStore store.Store) *ChatHandler {
	return &ChatHandler{
		publisher: publisher,
		store:     workflowStore,
		now:       time.Now,
	}
}

func (h *ChatHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()

	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	workflowID := newID()
	correlationID := newID()
	now := h.now().UTC()

	workflow := store.Workflow{
		ID:            workflowID,
		CorrelationID: correlationID,
		Prompt:        req.Prompt,
		Status:        "running",
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := h.store.CreateWorkflow(r.Context(), workflow); err != nil {
		http.Error(w, "failed to create workflow", http.StatusInternalServerError)
		return
	}

	event := eventschema.Event{
		ID:        newID(),
		Topic:     requestTopic,
		Status:    eventschema.EventStatusPending,
		Source:    "api-gateway",
		Timestamp: now,
		Payload: map[string]any{
			"event_name":     "request.received",
			"workflow_id":    workflowID,
			"correlation_id": correlationID,
			"prompt":         req.Prompt,
		},
	}
	if err := h.publisher.Publish(r.Context(), requestTopic, event); err != nil {
		http.Error(w, "failed to publish event", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(ChatResponse{
		WorkflowID:    workflowID,
		CorrelationID: correlationID,
	})
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}

type WorkflowHandler struct {
	publisher bus.Publisher
	store     store.Store
}

func NewWorkflowHandler(publisher bus.Publisher, workflowStore store.Store) *WorkflowHandler {
	return &WorkflowHandler{
		publisher: publisher,
		store:     workflowStore,
	}
}

func (h *WorkflowHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && isWorkflowCollectionPath(r.URL.Path):
		h.serveWorkflowHistory(w, r)
	case r.Method == http.MethodGet && isWorkflowRootPath(r.URL.Path):
		h.serveWorkflowSummary(w, r)
	case r.Method == http.MethodGet && hasWorkflowSuffix(r.URL.Path, "/events"):
		h.serveWorkflowEvents(w, r)
	case r.Method == http.MethodGet && hasWorkflowSuffix(r.URL.Path, "/stream"):
		h.serveWorkflowStream(w, r)
	case r.Method == http.MethodPost && hasWorkflowSuffix(r.URL.Path, "/approval"):
		h.serveWorkflowApproval(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *WorkflowHandler) serveWorkflowHistory(w http.ResponseWriter, r *http.Request) {
	workflows, err := h.store.ListWorkflows(r.Context(), 20)
	if err != nil {
		http.Error(w, "failed to load workflows", http.StatusInternalServerError)
		return
	}

	summaries := make([]WorkflowSummary, 0, len(workflows))
	for _, workflow := range workflows {
		events, err := h.store.ListWorkflowEvents(r.Context(), workflow.ID)
		if err != nil {
			http.Error(w, "failed to load workflow history", http.StatusInternalServerError)
			return
		}
		summaries = append(summaries, buildWorkflowSummary(workflow, events))
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(WorkflowHistoryResponse{Workflows: summaries})
}

func (h *WorkflowHandler) serveWorkflowSummary(w http.ResponseWriter, r *http.Request) {
	workflowID, ok := workflowRootIDFromPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	workflow, err := h.store.GetWorkflow(r.Context(), workflowID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "failed to load workflow", http.StatusInternalServerError)
		return
	}

	events, err := h.store.ListWorkflowEvents(r.Context(), workflowID)
	if err != nil {
		http.Error(w, "failed to load workflow events", http.StatusInternalServerError)
		return
	}

	summary := buildWorkflowSummary(workflow, events)
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(summary)
}

func (h *WorkflowHandler) serveWorkflowEvents(w http.ResponseWriter, r *http.Request) {
	workflowID, ok := workflowIDFromPath(r.URL.Path, "/events")
	if !ok {
		http.NotFound(w, r)
		return
	}

	events, err := h.store.ListWorkflowEvents(r.Context(), workflowID)
	if err != nil {
		http.Error(w, "failed to load workflow events", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(WorkflowEventsResponse{Events: events})
}

func (h *WorkflowHandler) serveWorkflowStream(w http.ResponseWriter, r *http.Request) {
	workflowID, ok := workflowIDFromPath(r.URL.Path, "/stream")
	if !ok {
		http.NotFound(w, r)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	cursorTime := time.Time{}
	cursorID := ""
	heartbeatTicker := time.NewTicker(15 * time.Second)
	defer heartbeatTicker.Stop()

	for {
		events, err := h.store.ListWorkflowEventsAfter(r.Context(), workflowID, cursorTime, cursorID)
		if err != nil {
			http.Error(w, "failed to stream workflow events", http.StatusInternalServerError)
			return
		}
		for _, event := range events {
			payload, err := json.Marshal(event)
			if err != nil {
				http.Error(w, "failed to encode workflow event", http.StatusInternalServerError)
				return
			}
			if _, err := w.Write([]byte("event: workflow-event\n")); err != nil {
				return
			}
			if _, err := w.Write([]byte("data: " + string(payload) + "\n\n")); err != nil {
				return
			}
			flusher.Flush()
			cursorTime = event.Timestamp
			cursorID = event.ID
		}

		select {
		case <-r.Context().Done():
			return
		case <-heartbeatTicker.C:
			if _, err := w.Write([]byte(": keepalive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (h *WorkflowHandler) serveWorkflowApproval(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	workflowID, ok := workflowIDFromPath(r.URL.Path, "/approval")
	if !ok {
		http.NotFound(w, r)
		return
	}

	var req ApprovalRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if req.Decision != "approved" && req.Decision != "rejected" {
		http.Error(w, "decision must be approved or rejected", http.StatusBadRequest)
		return
	}

	workflow, err := h.store.GetWorkflow(r.Context(), workflowID)
	if err != nil {
		if err == sql.ErrNoRows {
			http.NotFound(w, r)
			return
		}
		http.Error(w, "failed to load workflow", http.StatusInternalServerError)
		return
	}

	now := time.Now().UTC()
	event := eventschema.Event{
		ID:        newID(),
		Topic:     "mesh.approval.received",
		Status:    eventschema.EventStatusSucceeded,
		Source:    "api-gateway",
		Timestamp: now,
		Payload: map[string]any{
			"event_name":     "approval.received",
			"workflow_id":    workflow.ID,
			"correlation_id": workflow.CorrelationID,
			"decision":       req.Decision,
			"comment":        strings.TrimSpace(req.Comment),
		},
	}
	if err := h.publisher.Publish(r.Context(), event.Topic, event); err != nil {
		http.Error(w, "failed to publish approval event", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}

func hasWorkflowSuffix(path, suffix string) bool {
	_, ok := workflowIDFromPath(path, suffix)
	return ok
}

func isWorkflowCollectionPath(path string) bool {
	return path == "/api/workflows" || path == "/api/workflows/"
}

func isWorkflowRootPath(path string) bool {
	if isWorkflowCollectionPath(path) {
		return false
	}
	_, ok := workflowRootIDFromPath(path)
	return ok
}

func workflowRootIDFromPath(path string) (string, bool) {
	const prefix = "/api/workflows/"
	if !strings.HasPrefix(path, prefix) {
		return "", false
	}
	workflowID := strings.TrimPrefix(path, prefix)
	workflowID = strings.Trim(workflowID, "/")
	if workflowID == "" || strings.Contains(workflowID, "/") {
		return "", false
	}
	return workflowID, true
}

func workflowIDFromPath(path, suffix string) (string, bool) {
	const prefix = "/api/workflows/"

	if !strings.HasPrefix(path, prefix) || !strings.HasSuffix(path, suffix) {
		return "", false
	}

	workflowID := strings.TrimSuffix(strings.TrimPrefix(path, prefix), suffix)
	workflowID = strings.Trim(workflowID, "/")
	if workflowID == "" || strings.Contains(workflowID, "/") {
		return "", false
	}

	return workflowID, true
}

func buildWorkflowSummary(workflow store.Workflow, events []eventschema.Event) WorkflowSummary {
	summary := WorkflowSummary{
		WorkflowID:    workflow.ID,
		CorrelationID: workflow.CorrelationID,
		Prompt:        workflow.Prompt,
		Status:        workflow.Status,
		CreatedAt:     workflow.CreatedAt,
		UpdatedAt:     workflow.UpdatedAt,
		EventCount:    len(events),
	}

	var latest *eventschema.Event
	for index := range events {
		event := events[index]
		latest = &event

		switch event.Topic {
		case "mesh.plan":
			if intent, ok := event.Payload["intent"].(string); ok {
				summary.Intent = intent
			}
		case "mesh.approval.required":
			summary.ApprovalStatus = "pending"
		case "mesh.approval.received":
			summary.ApprovalStatus = "received"
			if decision, ok := event.Payload["decision"].(string); ok {
				summary.ApprovalDecision = decision
			}
			if comment, ok := event.Payload["comment"].(string); ok {
				summary.ApprovalComment = comment
			}
		case "mesh.result.retrieval":
			summary.RetrievalResult = clonePayloadMap(event.Payload)
		case "mesh.result.classification":
			summary.ClassificationResult = clonePayloadMap(event.Payload)
		case "mesh.result.response":
			summary.ResponseResult = clonePayloadMap(event.Payload)
		case "mesh.workflow.completed":
			summary.Status = "completed"
			if intent, ok := event.Payload["intent"].(string); ok {
				summary.Intent = intent
			}
			if result, ok := event.Payload["retrieval_result"].(map[string]any); ok {
				summary.RetrievalResult = result
			}
			if result, ok := event.Payload["classification_result"].(map[string]any); ok {
				summary.ClassificationResult = result
			}
			if result, ok := event.Payload["response_result"].(map[string]any); ok {
				summary.ResponseResult = result
			}
			if decision, ok := event.Payload["approval_decision"].(string); ok {
				summary.ApprovalDecision = decision
				summary.ApprovalStatus = "received"
			}
			if comment, ok := event.Payload["approval_comment"].(string); ok {
				summary.ApprovalComment = comment
			}
		case "mesh.workflow.failed":
			summary.Status = "failed"
			if reason, ok := event.Payload["reason"].(string); ok {
				summary.FailureReason = reason
			}
			if intent, ok := event.Payload["intent"].(string); ok {
				summary.Intent = intent
			}
			if result, ok := event.Payload["retrieval_result"].(map[string]any); ok {
				summary.RetrievalResult = result
			}
			if result, ok := event.Payload["classification_result"].(map[string]any); ok {
				summary.ClassificationResult = result
			}
			if result, ok := event.Payload["response_result"].(map[string]any); ok {
				summary.ResponseResult = result
			}
			if decision, ok := event.Payload["approval_decision"].(string); ok {
				summary.ApprovalDecision = decision
				summary.ApprovalStatus = "received"
			}
			if comment, ok := event.Payload["approval_comment"].(string); ok {
				summary.ApprovalComment = comment
			}
		}
	}

	summary.LatestEvent = latest
	return summary
}

func clonePayloadMap(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	clone := make(map[string]any, len(payload))
	for key, value := range payload {
		clone[key] = value
	}
	return clone
}
