package handlers

import (
	"crypto/rand"
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
	if err := h.store.SaveEvent(r.Context(), event); err != nil {
		http.Error(w, "failed to save event", http.StatusInternalServerError)
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

type WorkflowEventsHandler struct {
	store store.Store
}

func NewWorkflowEventsHandler(workflowStore store.Store) *WorkflowEventsHandler {
	return &WorkflowEventsHandler{store: workflowStore}
}

func (h *WorkflowEventsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}

	workflowID, ok := workflowIDFromPath(r.URL.Path)
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

func workflowIDFromPath(path string) (string, bool) {
	const prefix = "/api/workflows/"
	const suffix = "/events"

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
