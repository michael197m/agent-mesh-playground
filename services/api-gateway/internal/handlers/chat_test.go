package handlers

import "testing"

func TestWorkflowIDFromPath(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantID     string
		wantParsed bool
	}{
		{
			name:       "valid path",
			path:       "/api/workflows/workflow-123/events",
			wantID:     "workflow-123",
			wantParsed: true,
		},
		{
			name:       "missing workflow id",
			path:       "/api/workflows//events",
			wantParsed: false,
		},
		{
			name:       "nested segment",
			path:       "/api/workflows/workflow-123/extra/events",
			wantParsed: false,
		},
		{
			name:       "wrong suffix",
			path:       "/api/workflows/workflow-123",
			wantParsed: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotParsed := workflowIDFromPath(tt.path)
			if gotParsed != tt.wantParsed {
				t.Fatalf("parsed = %v, want %v", gotParsed, tt.wantParsed)
			}
			if gotID != tt.wantID {
				t.Fatalf("workflowID = %q, want %q", gotID, tt.wantID)
			}
		})
	}
}
