package aggregator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"agentmesh/services/aggregator-agent/internal/bus"
	"agentmesh/services/aggregator-agent/internal/store"
	eventschema "agentmesh/shared/event-schema"
)

const (
	aggregatorConsumerName    = "aggregator-agent"
	approvalRequiredTopic     = "mesh.approval.required"
	approvalReceivedTopic     = "mesh.approval.received"
	retrievalResultTopic      = "mesh.result.retrieval"
	classificationResultTopic = "mesh.result.classification"
	responseTaskTopic         = "mesh.task.response"
	responseResultTopic       = "mesh.result.response"
	workflowCompletedTopic    = "mesh.workflow.completed"
	workflowFailedTopic       = "mesh.workflow.failed"
	responseRequestedEvent    = "task.response.requested"
	workflowCompletedEvent    = "workflow.completed"
	workflowFailedEvent       = "workflow.failed"
)

type Bus interface {
	Poll(ctx context.Context, topic string, since time.Time, limit int) ([]eventschema.Event, error)
	Publish(ctx context.Context, event eventschema.Event) error
}

type Store interface {
	HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error)
	RecordResult(ctx context.Context, event eventschema.Event) (store.WorkflowState, error)
	MarkApprovalRequired(ctx context.Context, event eventschema.Event) (store.WorkflowState, error)
	MarkApprovalReceived(ctx context.Context, event eventschema.Event) (store.WorkflowState, error)
	MarkCompleted(ctx context.Context, correlationID, completionEventID string) error
	ListTimedOutWorkflows(ctx context.Context, deadline time.Time, limit int) ([]store.WorkflowState, error)
	MarkFailed(ctx context.Context, correlationID, failureEventID, failureReason string) error
	MarkResponseRequested(ctx context.Context, correlationID, responseTaskEventID string) error
	MarkProcessed(ctx context.Context, consumerName, eventID string) error
}

type Config struct {
	PollInterval    time.Duration
	WorkflowTimeout time.Duration
	Logger          *slog.Logger
	Bus             Bus
	Store           Store
}

type Agent struct {
	pollInterval    time.Duration
	workflowTimeout time.Duration
	logger          *slog.Logger
	bus             Bus
	store           Store
}

type CompletionDecision struct {
	Ready  bool
	Reason string
}

func EvaluateCompletion(state store.WorkflowState) CompletionDecision {
	if state.CompletedEventID != "" {
		return CompletionDecision{Ready: false, Reason: "workflow already completed"}
	}
	if state.FailedEventID != "" {
		return CompletionDecision{Ready: false, Reason: "workflow already failed"}
	}
	if state.ApprovalRequiredEventID != "" && state.ApprovalDecision == "" {
		return CompletionDecision{Ready: false, Reason: "waiting for operator approval"}
	}
	if state.ApprovalDecision == "rejected" {
		return CompletionDecision{Ready: false, Reason: "approval rejected"}
	}
	if state.RetrievalPayload == nil {
		return CompletionDecision{Ready: false, Reason: "missing retrieval result"}
	}
	if state.ClassificationPayload == nil {
		return CompletionDecision{Ready: false, Reason: "missing classification result"}
	}
	if state.ResponsePayload == nil {
		if state.ResponseRequestedEventID != "" {
			return CompletionDecision{Ready: false, Reason: "waiting for synthesized response"}
		}
		return CompletionDecision{Ready: false, Reason: "response task has not been requested yet"}
	}
	return CompletionDecision{Ready: true, Reason: "all required results present"}
}

func NewAgent(cfg Config) (*Agent, error) {
	if cfg.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if cfg.Bus == nil {
		return nil, fmt.Errorf("bus is required")
	}
	if cfg.Store == nil {
		return nil, fmt.Errorf("store is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.WorkflowTimeout <= 0 {
		cfg.WorkflowTimeout = 45 * time.Second
	}

	return &Agent{
		pollInterval:    cfg.PollInterval,
		workflowTimeout: cfg.WorkflowTimeout,
		logger:          cfg.Logger,
		bus:             cfg.Bus,
		store:           cfg.Store,
	}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	lastSeen := map[string]time.Time{
		approvalRequiredTopic:     {},
		approvalReceivedTopic:     {},
		retrievalResultTopic:      {},
		classificationResultTopic: {},
		responseResultTopic:       {},
	}
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		for _, topic := range []string{approvalRequiredTopic, approvalReceivedTopic, retrievalResultTopic, classificationResultTopic, responseResultTopic} {
			events, err := a.bus.Poll(ctx, topic, lastSeen[topic], 100)
			if err != nil {
				return fmt.Errorf("poll %s events: %w", topic, err)
			}
			for _, event := range events {
				if event.Timestamp.After(lastSeen[topic]) {
					lastSeen[topic] = event.Timestamp
				}

				processed, err := a.store.HasProcessed(ctx, aggregatorConsumerName, event.ID)
				if err != nil {
					return fmt.Errorf("check inbox for %s: %w", topic, err)
				}
				if processed {
					a.logger.Info("result event already processed", "event_id", event.ID, "topic", event.Topic)
					continue
				}

				if err := a.handleEvent(ctx, event); err != nil {
					a.logger.Error("failed to process workflow event", "error", err, "event_id", event.ID, "topic", event.Topic)
					continue
				}
				if err := a.store.MarkProcessed(ctx, aggregatorConsumerName, event.ID); err != nil {
					return fmt.Errorf("mark %s processed: %w", topic, err)
				}
			}
		}
		if err := a.failTimedOutWorkflows(ctx); err != nil {
			return fmt.Errorf("fail timed out workflows: %w", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *Agent) failTimedOutWorkflows(ctx context.Context) error {
	states, err := a.store.ListTimedOutWorkflows(ctx, time.Now().UTC().Add(-a.workflowTimeout), 100)
	if err != nil {
		return err
	}

	for _, state := range states {
		failureReason := fmt.Sprintf("workflow timed out after %s waiting for all required results", a.workflowTimeout)
		failureEvent := newFailedEvent(state, failureReason)

		if err := a.bus.Publish(ctx, failureEvent); err != nil {
			return fmt.Errorf("publish workflow failure for %s: %w", state.CorrelationID, err)
		}
		if err := a.store.MarkFailed(ctx, state.CorrelationID, failureEvent.ID, failureReason); err != nil {
			return fmt.Errorf("mark workflow failed for %s: %w", state.CorrelationID, err)
		}

		a.logger.Warn("workflow failed",
			"workflow_id", state.WorkflowID,
			"correlation_id", state.CorrelationID,
			"failure_event_id", failureEvent.ID,
			"reason", failureReason,
		)
	}

	return nil
}

func (a *Agent) handleEvent(ctx context.Context, event eventschema.Event) error {
	switch event.Topic {
	case approvalRequiredTopic:
		return a.handleApprovalRequiredEvent(ctx, event)
	case approvalReceivedTopic:
		return a.handleApprovalReceivedEvent(ctx, event)
	default:
		return a.handleResultEvent(ctx, event)
	}
}

func (a *Agent) handleResultEvent(ctx context.Context, event eventschema.Event) error {
	correlationID, _ := event.Payload["correlation_id"].(string)
	workflowID, _ := event.Payload["workflow_id"].(string)
	if correlationID == "" || workflowID == "" {
		return fmt.Errorf("result event missing workflow identifiers")
	}

	a.logger.Info("processing result event",
		"event_id", event.ID,
		"topic", event.Topic,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"payload", bus.PayloadKey(event.Payload),
	)

	state, err := a.store.RecordResult(ctx, event)
	if err != nil {
		return fmt.Errorf("record result event: %w", err)
	}

	decision := EvaluateCompletion(state)
	if !decision.Ready && shouldRequestResponse(state) {
		responseTask := newResponseTaskEvent(state)
		if err := a.bus.Publish(ctx, responseTask); err != nil {
			return fmt.Errorf("publish response task: %w", err)
		}
		if err := a.store.MarkResponseRequested(ctx, state.CorrelationID, responseTask.ID); err != nil {
			return fmt.Errorf("mark response requested: %w", err)
		}

		a.logger.Info("response task requested",
			"workflow_id", state.WorkflowID,
			"correlation_id", state.CorrelationID,
			"response_task_event_id", responseTask.ID,
		)
		return nil
	}
	if !decision.Ready {
		a.logger.Info("workflow not complete yet", "correlation_id", correlationID, "reason", decision.Reason)
		return nil
	}

	completionEvent := newCompletionEvent(state)
	if err := a.bus.Publish(ctx, completionEvent); err != nil {
		return fmt.Errorf("publish workflow completion: %w", err)
	}
	if err := a.store.MarkCompleted(ctx, correlationID, completionEvent.ID); err != nil {
		return fmt.Errorf("mark workflow completion: %w", err)
	}

	a.logger.Info("workflow completed",
		"workflow_id", state.WorkflowID,
		"correlation_id", state.CorrelationID,
		"completion_event_id", completionEvent.ID,
	)

	return nil
}

func (a *Agent) handleApprovalRequiredEvent(ctx context.Context, event eventschema.Event) error {
	correlationID, _ := event.Payload["correlation_id"].(string)
	workflowID, _ := event.Payload["workflow_id"].(string)
	if correlationID == "" || workflowID == "" {
		return fmt.Errorf("approval required event missing workflow identifiers")
	}

	state, err := a.store.MarkApprovalRequired(ctx, event)
	if err != nil {
		return fmt.Errorf("record approval required event: %w", err)
	}

	a.logger.Info("approval required",
		"workflow_id", state.WorkflowID,
		"correlation_id", state.CorrelationID,
		"event_id", event.ID,
	)

	return nil
}

func (a *Agent) handleApprovalReceivedEvent(ctx context.Context, event eventschema.Event) error {
	correlationID, _ := event.Payload["correlation_id"].(string)
	workflowID, _ := event.Payload["workflow_id"].(string)
	if correlationID == "" || workflowID == "" {
		return fmt.Errorf("approval received event missing workflow identifiers")
	}

	state, err := a.store.MarkApprovalReceived(ctx, event)
	if err != nil {
		return fmt.Errorf("record approval received event: %w", err)
	}

	if state.ApprovalDecision == "rejected" {
		failureReason := "workflow rejected by operator approval gate"
		failureEvent := newFailedEvent(state, failureReason)
		if err := a.bus.Publish(ctx, failureEvent); err != nil {
			return fmt.Errorf("publish workflow failure after rejection: %w", err)
		}
		if err := a.store.MarkFailed(ctx, state.CorrelationID, failureEvent.ID, failureReason); err != nil {
			return fmt.Errorf("mark workflow failed after rejection: %w", err)
		}

		a.logger.Warn("workflow rejected by approval",
			"workflow_id", state.WorkflowID,
			"correlation_id", state.CorrelationID,
			"failure_event_id", failureEvent.ID,
		)
		return nil
	}

	decision := EvaluateCompletion(state)
	if !decision.Ready && shouldRequestResponse(state) {
		responseTask := newResponseTaskEvent(state)
		if err := a.bus.Publish(ctx, responseTask); err != nil {
			return fmt.Errorf("publish response task after approval: %w", err)
		}
		if err := a.store.MarkResponseRequested(ctx, state.CorrelationID, responseTask.ID); err != nil {
			return fmt.Errorf("mark response requested after approval: %w", err)
		}
	}

	a.logger.Info("approval received",
		"workflow_id", state.WorkflowID,
		"correlation_id", state.CorrelationID,
		"decision", state.ApprovalDecision,
	)

	return nil
}

func shouldRequestResponse(state store.WorkflowState) bool {
	return state.CompletedEventID == "" &&
		state.FailedEventID == "" &&
		(state.ApprovalRequiredEventID == "" || state.ApprovalDecision == "approved") &&
		state.RetrievalPayload != nil &&
		state.ClassificationPayload != nil &&
		state.ResponsePayload == nil &&
		state.ResponseRequestedEventID == ""
}

func newCompletionEvent(state store.WorkflowState) eventschema.Event {
	payload := map[string]any{
		"event_name":            workflowCompletedEvent,
		"workflow_id":           state.WorkflowID,
		"correlation_id":        state.CorrelationID,
		"retrieval_result":      state.RetrievalPayload,
		"classification_result": state.ClassificationPayload,
		"response_result":       state.ResponsePayload,
	}
	if state.Intent != "" {
		payload["intent"] = state.Intent
	}
	if state.ApprovalDecision != "" {
		payload["approval_decision"] = state.ApprovalDecision
	}
	if state.ApprovalComment != "" {
		payload["approval_comment"] = state.ApprovalComment
	}

	return eventschema.Event{
		ID:        newID(),
		Topic:     workflowCompletedTopic,
		Status:    eventschema.EventStatusSucceeded,
		Source:    "aggregator-agent",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

func newFailedEvent(state store.WorkflowState, reason string) eventschema.Event {
	payload := map[string]any{
		"event_name":     workflowFailedEvent,
		"workflow_id":    state.WorkflowID,
		"correlation_id": state.CorrelationID,
		"reason":         reason,
	}
	if state.Intent != "" {
		payload["intent"] = state.Intent
	}
	if state.ApprovalDecision != "" {
		payload["approval_decision"] = state.ApprovalDecision
	}
	if state.ApprovalComment != "" {
		payload["approval_comment"] = state.ApprovalComment
	}
	if state.RetrievalPayload != nil {
		payload["retrieval_result"] = state.RetrievalPayload
	}
	if state.ClassificationPayload != nil {
		payload["classification_result"] = state.ClassificationPayload
	}
	if state.ResponsePayload != nil {
		payload["response_result"] = state.ResponsePayload
	}

	return eventschema.Event{
		ID:        newID(),
		Topic:     workflowFailedTopic,
		Status:    eventschema.EventStatusFailed,
		Source:    "aggregator-agent",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

func newResponseTaskEvent(state store.WorkflowState) eventschema.Event {
	payload := map[string]any{
		"event_name":            responseRequestedEvent,
		"workflow_id":           state.WorkflowID,
		"correlation_id":        state.CorrelationID,
		"task_type":             "response",
		"prompt":                state.Prompt,
		"reason":                "synthesize final response from retrieval and classification results",
		"retrieval_context":     payloadString(state.RetrievalPayload, "retrieval_context"),
		"classification_result": payloadString(state.ClassificationPayload, "classification_result"),
	}
	if state.Intent != "" {
		payload["intent"] = state.Intent
	}

	return eventschema.Event{
		ID:        newID(),
		Topic:     responseTaskTopic,
		Status:    eventschema.EventStatusPending,
		Source:    "aggregator-agent",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

func payloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return value
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
