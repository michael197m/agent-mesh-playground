package aggregator

import (
	"testing"

	"agentmesh/services/aggregator-agent/internal/store"
)

func TestEvaluateCompletion(t *testing.T) {
	tests := []struct {
		name   string
		state  store.WorkflowState
		ready  bool
		reason string
	}{
		{
			name:   "already completed",
			state:  store.WorkflowState{CompletedEventID: "done"},
			ready:  false,
			reason: "workflow already completed",
		},
		{
			name:   "already failed",
			state:  store.WorkflowState{FailedEventID: "failed"},
			ready:  false,
			reason: "workflow already failed",
		},
		{
			name:   "missing retrieval",
			state:  store.WorkflowState{},
			ready:  false,
			reason: "missing retrieval result",
		},
		{
			name:   "missing classification",
			state:  store.WorkflowState{RetrievalPayload: map[string]any{"ok": true}},
			ready:  false,
			reason: "missing classification result",
		},
		{
			name: "missing response",
			state: store.WorkflowState{
				RetrievalPayload:      map[string]any{"ok": true},
				ClassificationPayload: map[string]any{"ok": true},
			},
			ready:  false,
			reason: "response task has not been requested yet",
		},
		{
			name: "waiting for synthesized response",
			state: store.WorkflowState{
				RetrievalPayload:         map[string]any{"ok": true},
				ClassificationPayload:    map[string]any{"ok": true},
				ResponseRequestedEventID: "response-task",
			},
			ready:  false,
			reason: "waiting for synthesized response",
		},
		{
			name: "ready",
			state: store.WorkflowState{
				RetrievalPayload:      map[string]any{"ok": true},
				ClassificationPayload: map[string]any{"ok": true},
				ResponsePayload:       map[string]any{"ok": true},
			},
			ready:  true,
			reason: "all required results present",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := EvaluateCompletion(tt.state)
			if decision.Ready != tt.ready {
				t.Fatalf("ready = %v, want %v", decision.Ready, tt.ready)
			}
			if decision.Reason != tt.reason {
				t.Fatalf("reason = %q, want %q", decision.Reason, tt.reason)
			}
		})
	}
}

func TestShouldRequestResponse(t *testing.T) {
	tests := []struct {
		name  string
		state store.WorkflowState
		want  bool
	}{
		{
			name: "ready to request response",
			state: store.WorkflowState{
				RetrievalPayload:      map[string]any{"retrieval_context": "docs"},
				ClassificationPayload: map[string]any{"classification_result": "severity=low"},
			},
			want: true,
		},
		{
			name: "missing classification",
			state: store.WorkflowState{
				RetrievalPayload: map[string]any{"retrieval_context": "docs"},
			},
			want: false,
		},
		{
			name: "already requested",
			state: store.WorkflowState{
				RetrievalPayload:         map[string]any{"retrieval_context": "docs"},
				ClassificationPayload:    map[string]any{"classification_result": "severity=low"},
				ResponseRequestedEventID: "response-task",
			},
			want: false,
		},
		{
			name: "already completed",
			state: store.WorkflowState{
				RetrievalPayload:      map[string]any{"retrieval_context": "docs"},
				ClassificationPayload: map[string]any{"classification_result": "severity=low"},
				CompletedEventID:      "done",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldRequestResponse(tt.state)
			if got != tt.want {
				t.Fatalf("shouldRequestResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}
