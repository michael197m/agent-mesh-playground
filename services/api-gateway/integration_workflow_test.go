//go:build integration

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	eventschema "agentmesh/shared/event-schema"
)

type integrationChatResponse struct {
	WorkflowID    string `json:"workflow_id"`
	CorrelationID string `json:"correlation_id"`
}

type integrationWorkflowEventsResponse struct {
	Events []eventschema.Event `json:"events"`
}

type ollamaChatRequest struct {
	Messages []struct {
		Role    string `json:"role"`
		Content string `json:"content"`
	} `json:"messages"`
}

type serviceProcess struct {
	name    string
	cmd     *exec.Cmd
	logPath string
}

type fakeServer struct {
	URL   string
	close func()
}

func (s *fakeServer) Close() {
	if s != nil && s.close != nil {
		s.close()
	}
}

func TestWorkflowCompletesAcrossServices(t *testing.T) {
	ctx, apiURL, processes := startWorkflowHarness(t, true, "20s")
	defer func() {
		if t.Failed() {
			t.Log(dumpServiceLogs(processes))
		}
	}()
	defer stopServices(t, processes)

	workflow := submitWorkflow(t, apiURL, "Our Toronto edge site is showing intermittent packet loss after a config update. Classify severity, retrieve relevant context, and recommend next steps.")
	events := waitForTerminalWorkflow(t, ctx, apiURL, workflow.WorkflowID)

	assertHasTopic(t, events, "mesh.request")
	assertHasTopic(t, events, "mesh.plan")
	assertHasTopic(t, events, "mesh.task.retrieval")
	assertHasTopic(t, events, "mesh.task.classification")
	assertHasTopic(t, events, "mesh.result.retrieval")
	assertHasTopic(t, events, "mesh.result.classification")
	assertHasTopic(t, events, "mesh.task.response")
	assertHasTopic(t, events, "mesh.result.response")

	completion := lastEventForTopic(events, "mesh.workflow.completed")
	if completion == nil {
		t.Fatalf("expected mesh.workflow.completed event; events=%v", eventTopics(events))
	}

	responseResult, _ := completion.Payload["response_result"].(map[string]any)
	responseText, _ := responseResult["response"].(string)
	if !strings.Contains(strings.ToLower(responseText), "packet loss") {
		t.Fatalf("response_result.response = %q, want packet loss context", responseText)
	}

	if failure := lastEventForTopic(events, "mesh.workflow.failed"); failure != nil {
		t.Fatalf("workflow unexpectedly failed: %+v", failure.Payload)
	}
}

func TestWorkflowFailsWhenResponderIsUnavailable(t *testing.T) {
	ctx, apiURL, processes := startWorkflowHarness(t, false, "8s")
	defer func() {
		if t.Failed() {
			t.Log(dumpServiceLogs(processes))
		}
	}()
	defer stopServices(t, processes)

	workflow := submitWorkflow(t, apiURL, "Our Toronto edge site is showing intermittent packet loss after a config update. Classify severity, retrieve relevant context, and recommend next steps.")
	events := waitForTerminalWorkflow(t, ctx, apiURL, workflow.WorkflowID)

	assertHasTopic(t, events, "mesh.request")
	assertHasTopic(t, events, "mesh.plan")
	assertHasTopic(t, events, "mesh.result.retrieval")
	assertHasTopic(t, events, "mesh.result.classification")
	assertHasTopic(t, events, "mesh.task.response")
	assertHasTopic(t, events, "mesh.workflow.failed")

	if completion := lastEventForTopic(events, "mesh.workflow.completed"); completion != nil {
		t.Fatalf("workflow unexpectedly completed: %+v", completion.Payload)
	}

	failure := lastEventForTopic(events, "mesh.workflow.failed")
	if failure == nil {
		t.Fatalf("expected mesh.workflow.failed event; events=%v", eventTopics(events))
	}

	reason, _ := failure.Payload["reason"].(string)
	if !strings.Contains(reason, "timed out") {
		t.Fatalf("failure reason = %q, want timeout message", reason)
	}

	if response := lastEventForTopic(events, "mesh.result.response"); response != nil {
		t.Fatalf("unexpected response event when responder is unavailable: %+v", response.Payload)
	}
}

func TestWorkflowCompletesAfterApprovalIsGranted(t *testing.T) {
	ctx, apiURL, processes := startWorkflowHarness(t, true, "20s")
	defer func() {
		if t.Failed() {
			t.Log(dumpServiceLogs(processes))
		}
	}()
	defer stopServices(t, processes)

	workflow := submitWorkflow(t, apiURL, "Review packet loss and prepare a rollback recommendation for the production edge firewall if the latest config is the cause.")
	preApprovalEvents := waitForWorkflowCondition(t, ctx, apiURL, workflow.WorkflowID, func(events []eventschema.Event) bool {
		return lastEventForTopic(events, "mesh.approval.required") != nil &&
			lastEventForTopic(events, "mesh.result.retrieval") != nil &&
			lastEventForTopic(events, "mesh.result.classification") != nil
	})

	assertHasTopic(t, preApprovalEvents, "mesh.approval.required")
	if responseTask := lastEventForTopic(preApprovalEvents, "mesh.task.response"); responseTask != nil {
		t.Fatalf("response task should not be requested before approval: %+v", responseTask.Payload)
	}

	submitApprovalDecision(t, apiURL, workflow.WorkflowID, "approved", "Rollback approved for production edge after validation.")

	events := waitForTerminalWorkflow(t, ctx, apiURL, workflow.WorkflowID)
	assertHasTopic(t, events, "mesh.approval.received")
	assertHasTopic(t, events, "mesh.task.response")
	assertHasTopic(t, events, "mesh.result.response")
	assertHasTopic(t, events, "mesh.workflow.completed")

	completion := lastEventForTopic(events, "mesh.workflow.completed")
	if completion == nil {
		t.Fatalf("expected completion after approval; events=%v", eventTopics(events))
	}
	if decision, _ := completion.Payload["approval_decision"].(string); decision != "approved" {
		t.Fatalf("approval_decision = %q, want approved", decision)
	}
}

func TestWorkflowFailsAfterApprovalIsRejected(t *testing.T) {
	ctx, apiURL, processes := startWorkflowHarness(t, true, "20s")
	defer func() {
		if t.Failed() {
			t.Log(dumpServiceLogs(processes))
		}
	}()
	defer stopServices(t, processes)

	workflow := submitWorkflow(t, apiURL, "Review packet loss and prepare a rollback recommendation for the production edge firewall if the latest config is the cause.")
	preApprovalEvents := waitForWorkflowCondition(t, ctx, apiURL, workflow.WorkflowID, func(events []eventschema.Event) bool {
		return lastEventForTopic(events, "mesh.approval.required") != nil &&
			lastEventForTopic(events, "mesh.result.retrieval") != nil &&
			lastEventForTopic(events, "mesh.result.classification") != nil
	})

	assertHasTopic(t, preApprovalEvents, "mesh.approval.required")
	submitApprovalDecision(t, apiURL, workflow.WorkflowID, "rejected", "Rejecting rollback until blast radius review is complete.")

	events := waitForTerminalWorkflow(t, ctx, apiURL, workflow.WorkflowID)
	assertHasTopic(t, events, "mesh.approval.received")
	assertHasTopic(t, events, "mesh.workflow.failed")

	if completion := lastEventForTopic(events, "mesh.workflow.completed"); completion != nil {
		t.Fatalf("workflow unexpectedly completed after approval rejection: %+v", completion.Payload)
	}
	if response := lastEventForTopic(events, "mesh.result.response"); response != nil {
		t.Fatalf("workflow should not produce a response after approval rejection: %+v", response.Payload)
	}

	failure := lastEventForTopic(events, "mesh.workflow.failed")
	if failure == nil {
		t.Fatalf("expected failure after rejection; events=%v", eventTopics(events))
	}
	if reason, _ := failure.Payload["reason"].(string); !strings.Contains(strings.ToLower(reason), "rejected") {
		t.Fatalf("failure reason = %q, want rejection message", reason)
	}
	if decision, _ := failure.Payload["approval_decision"].(string); decision != "rejected" {
		t.Fatalf("approval_decision = %q, want rejected", decision)
	}
}

func startWorkflowHarness(t *testing.T, includeResponder bool, workflowTimeout string) (context.Context, string, []*serviceProcess) {
	t.Helper()

	repoRoot := repoRootFromWD(t)
	runtimeDir := t.TempDir()
	dbPath := filepath.Join(runtimeDir, "mesh.db")
	docsDir := filepath.Join(runtimeDir, "docs")
	if err := os.MkdirAll(docsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", docsDir, err)
	}
	if err := os.WriteFile(filepath.Join(docsDir, "runbook.md"), []byte(`# Runbook

Packet loss after config updates: verify MTU, rollback recent changes, inspect interface errors, and confirm the previous known-good config.
`), 0o644); err != nil {
		t.Fatalf("WriteFile(runbook.md): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	ollamaServer := newFakeOllamaServer()
	t.Cleanup(ollamaServer.Close)

	buildCacheDir := filepath.Join(runtimeDir, "gocache")
	if err := os.MkdirAll(buildCacheDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", buildCacheDir, err)
	}

	apiAddr := mustFreeLocalAddr(t)
	apiURL := "http://" + apiAddr

	processes := []*serviceProcess{
		buildAndStartService(t, ctx, repoRoot, "api-gateway", filepath.Join(repoRoot, "services/api-gateway"), map[string]string{
			"API_GATEWAY_DB_PATH": dbPath,
			"API_GATEWAY_ADDR":    apiAddr,
			"GOCACHE":             filepath.Join(buildCacheDir, "api-gateway"),
		}),
		buildAndStartService(t, ctx, repoRoot, "planner-agent", filepath.Join(repoRoot, "services/planner-agent"), map[string]string{
			"PLANNER_DB_PATH": dbPath,
			"OLLAMA_BASE_URL": ollamaServer.URL,
			"OLLAMA_MODEL":    "test-model",
			"GOCACHE":         filepath.Join(buildCacheDir, "planner-agent"),
		}),
		buildAndStartService(t, ctx, repoRoot, "retriever-agent", filepath.Join(repoRoot, "services/retriever-agent"), map[string]string{
			"RETRIEVER_DB_PATH":   dbPath,
			"RETRIEVER_DOCS_PATH": docsDir,
			"GOCACHE":             filepath.Join(buildCacheDir, "retriever-agent"),
		}),
		buildAndStartService(t, ctx, repoRoot, "classifier-agent", filepath.Join(repoRoot, "services/classifier-agent"), map[string]string{
			"CLASSIFIER_DB_PATH": dbPath,
			"OLLAMA_BASE_URL":    ollamaServer.URL,
			"OLLAMA_MODEL":       "test-model",
			"GOCACHE":            filepath.Join(buildCacheDir, "classifier-agent"),
		}),
	}

	if includeResponder {
		processes = append(processes, buildAndStartService(t, ctx, repoRoot, "responder-agent", filepath.Join(repoRoot, "services/responder-agent"), map[string]string{
			"RESPONDER_DB_PATH": dbPath,
			"OLLAMA_BASE_URL":   ollamaServer.URL,
			"OLLAMA_MODEL":      "test-model",
			"GOCACHE":           filepath.Join(buildCacheDir, "responder-agent"),
		}))
	}

	processes = append(processes, buildAndStartService(t, ctx, repoRoot, "aggregator-agent", filepath.Join(repoRoot, "services/aggregator-agent"), map[string]string{
		"AGGREGATOR_DB_PATH":          dbPath,
		"AGGREGATOR_WORKFLOW_TIMEOUT": workflowTimeout,
		"GOCACHE":                     filepath.Join(buildCacheDir, "aggregator-agent"),
	}))

	waitForGateway(t, ctx, apiURL)
	return ctx, apiURL, processes
}

func newFakeOllamaServer() *fakeServer {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/chat" {
			http.NotFound(w, r)
			return
		}

		defer r.Body.Close()

		var req ollamaChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		systemPrompt := ""
		userPrompt := ""
		if len(req.Messages) > 0 {
			systemPrompt = req.Messages[0].Content
		}
		if len(req.Messages) > 1 {
			userPrompt = req.Messages[1].Content
		}

		content := fakeOllamaResponse(systemPrompt, userPrompt)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"message": map[string]any{
				"content": content,
			},
		})
	})

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	server := &http.Server{Handler: handler}
	go func() {
		_ = server.Serve(listener)
	}()

	return &fakeServer{
		URL: "http://" + listener.Addr().String(),
		close: func() {
			_ = server.Close()
			_ = listener.Close()
		},
	}
}

func fakeOllamaResponse(systemPrompt, userPrompt string) string {
	switch {
	case strings.Contains(systemPrompt, "planner agent"):
		return `{"intent":"incident triage","tasks":[{"type":"retrieval","reason":"collect runbook context"},{"type":"classification","reason":"classify the issue severity and category"},{"type":"response","reason":"prepare the final operator response"}]}`
	case strings.Contains(systemPrompt, "classifier agent"):
		return `{"severity":"high","category":"network-config","confidence":0.93,"reason":"packet loss started after a config update"}`
	case strings.Contains(systemPrompt, "responder agent"):
		if strings.Contains(strings.ToLower(userPrompt), "packet loss") {
			return `{"response":"Packet loss likely relates to the recent config change. Validate MTU and interface errors, compare against the previous config, and roll back if impact persists.","recommended_actions":["Compare the active config with the last known-good version.","Check MTU and interface error counters on the affected edge site.","Roll back the recent change if customer impact is ongoing."]}`
		}
		return `{"response":"Investigate the incident using the retrieved context and classification output.","recommended_actions":["Review the workflow inputs.","Validate the latest configuration changes."]}`
	default:
		return `{"response":"unexpected prompt","recommended_actions":["inspect test harness"]}`
	}
}

func repoRootFromWD(t *testing.T) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd(): %v", err)
	}

	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func mustFreeLocalAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen(): %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	return addr
}

func buildAndStartService(t *testing.T, ctx context.Context, repoRoot, name, serviceDir string, env map[string]string) *serviceProcess {
	t.Helper()

	buildCache := env["GOCACHE"]
	if buildCache != "" {
		if err := os.MkdirAll(buildCache, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", buildCache, err)
		}
	}

	binPath := filepath.Join(t.TempDir(), name)
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", binPath, ".")
	buildCmd.Dir = serviceDir
	buildCmd.Env = append(os.Environ(), envPairs(env)...)
	if output, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, string(output))
	}

	logPath := filepath.Join(t.TempDir(), name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("Create(%q): %v", logPath, err)
	}

	cmd := exec.CommandContext(ctx, binPath)
	cmd.Dir = serviceDir
	cmd.Env = append(os.Environ(), envPairs(env)...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	if err := logFile.Close(); err != nil {
		t.Fatalf("close log file for %s: %v", name, err)
	}

	return &serviceProcess{
		name:    name,
		cmd:     cmd,
		logPath: logPath,
	}
}

func stopServices(t *testing.T, processes []*serviceProcess) {
	t.Helper()

	for _, process := range processes {
		if process == nil || process.cmd == nil || process.cmd.Process == nil {
			continue
		}
		_ = process.cmd.Process.Kill()
		_ = process.cmd.Wait()
	}
}

func waitForGateway(t *testing.T, ctx context.Context, apiURL string) {
	t.Helper()

	client := &http.Client{Timeout: 500 * time.Millisecond}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodOptions, apiURL+"/api/chat", nil)
		if err != nil {
			t.Fatalf("NewRequestWithContext(): %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusNoContent {
				return
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("gateway did not become ready before timeout: %v", ctx.Err())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func submitWorkflow(t *testing.T, apiURL, prompt string) integrationChatResponse {
	t.Helper()

	body := strings.NewReader(fmt.Sprintf(`{"prompt":%q}`, prompt))
	resp, err := http.Post(apiURL+"/api/chat", "application/json", body)
	if err != nil {
		t.Fatalf("POST /api/chat: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /api/chat status = %d, want %d; body=%s", resp.StatusCode, http.StatusAccepted, strings.TrimSpace(string(payload)))
	}

	var parsed integrationChatResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		t.Fatalf("decode chat response: %v", err)
	}
	if parsed.WorkflowID == "" || parsed.CorrelationID == "" {
		t.Fatalf("chat response missing identifiers: %+v", parsed)
	}

	return parsed
}

func waitForTerminalWorkflow(t *testing.T, ctx context.Context, apiURL, workflowID string) []eventschema.Event {
	return waitForWorkflowCondition(t, ctx, apiURL, workflowID, func(events []eventschema.Event) bool {
		return lastEventForTopic(events, "mesh.workflow.completed") != nil || lastEventForTopic(events, "mesh.workflow.failed") != nil
	})
}

func waitForWorkflowCondition(t *testing.T, ctx context.Context, apiURL, workflowID string, done func([]eventschema.Event) bool) []eventschema.Event {
	t.Helper()

	client := &http.Client{Timeout: 2 * time.Second}
	var lastEvents []eventschema.Event

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/api/workflows/"+workflowID+"/events", nil)
		if err != nil {
			t.Fatalf("NewRequestWithContext(): %v", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			func() {
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK {
					return
				}
				var parsed integrationWorkflowEventsResponse
				if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
					t.Fatalf("decode workflow events: %v", err)
				}
				lastEvents = parsed.Events
			}()
			if done(lastEvents) {
				return lastEvents
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("workflow %s did not reach terminal state before timeout; topics=%v", workflowID, eventTopics(lastEvents))
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func submitApprovalDecision(t *testing.T, apiURL, workflowID, decision, comment string) {
	t.Helper()

	body := strings.NewReader(fmt.Sprintf(`{"decision":%q,"comment":%q}`, decision, comment))
	req, err := http.NewRequest(http.MethodPost, apiURL+"/api/workflows/"+workflowID+"/approval", body)
	if err != nil {
		t.Fatalf("NewRequest(approval): %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /api/workflows/%s/approval: %v", workflowID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		payload, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST approval status = %d, want %d; body=%s", resp.StatusCode, http.StatusAccepted, strings.TrimSpace(string(payload)))
	}
}

func assertHasTopic(t *testing.T, events []eventschema.Event, topic string) {
	t.Helper()

	if lastEventForTopic(events, topic) == nil {
		t.Fatalf("expected topic %q in workflow events, got %v", topic, eventTopics(events))
	}
}

func lastEventForTopic(events []eventschema.Event, topic string) *eventschema.Event {
	for index := len(events) - 1; index >= 0; index-- {
		if events[index].Topic == topic {
			return &events[index]
		}
	}
	return nil
}

func eventTopics(events []eventschema.Event) []string {
	topics := make([]string, 0, len(events))
	for _, event := range events {
		topics = append(topics, event.Topic)
	}
	return topics
}

func envPairs(values map[string]string) []string {
	pairs := make([]string, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, key+"="+value)
	}
	return pairs
}

func dumpServiceLogs(processes []*serviceProcess) string {
	var output bytes.Buffer

	for _, process := range processes {
		if process == nil {
			continue
		}
		content, err := os.ReadFile(process.logPath)
		output.WriteString("\n== " + process.name + " ==\n")
		if err != nil {
			output.WriteString("failed to read logs: " + err.Error() + "\n")
			continue
		}
		output.Write(content)
	}

	return output.String()
}
