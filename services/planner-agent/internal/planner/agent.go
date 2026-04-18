package planner

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"agentmesh/services/planner-agent/internal/bus"
	eventschema "agentmesh/shared/event-schema"
)

const (
	plannerConsumerName      = "planner-agent"
	requestTopic             = "mesh.request"
	planTopic                = "mesh.plan"
	retrievalTopic           = "mesh.task.retrieval"
	classificationTopic      = "mesh.task.classification"
	approvalTopic            = "mesh.approval.required"
	responseTopic            = "mesh.task.response"
	requestReceivedEventName = "request.received"
	planCreatedEventName     = "plan.created"
)

type Bus interface {
	Poll(ctx context.Context, topic string, since time.Time, limit int) ([]eventschema.Event, error)
	Publish(ctx context.Context, event eventschema.Event) error
	HasProcessed(ctx context.Context, consumerName, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, consumerName, eventID string) error
}

type OllamaClient interface {
	Chat(ctx context.Context, systemPrompt, userPrompt string) (string, error)
}

type Config struct {
	PromptPath   string
	PollInterval time.Duration
	Logger       *slog.Logger
	Bus          Bus
	OllamaClient OllamaClient
}

type Agent struct {
	prompt       string
	pollInterval time.Duration
	logger       *slog.Logger
	bus          Bus
	ollamaClient OllamaClient
}

type plannerOutput struct {
	Intent string        `json:"intent"`
	Tasks  []plannedTask `json:"tasks"`
}

type plannedTask struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func NewAgent(cfg Config) (*Agent, error) {
	prompt, err := os.ReadFile(cfg.PromptPath)
	if err != nil {
		return nil, fmt.Errorf("read planner prompt: %w", err)
	}
	if cfg.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if cfg.Bus == nil {
		return nil, fmt.Errorf("bus is required")
	}
	if cfg.OllamaClient == nil {
		return nil, fmt.Errorf("ollama client is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}

	return &Agent{
		prompt:       string(prompt),
		pollInterval: cfg.PollInterval,
		logger:       cfg.Logger,
		bus:          cfg.Bus,
		ollamaClient: cfg.OllamaClient,
	}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	lastSeen := time.Time{}
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		events, err := a.bus.Poll(ctx, requestTopic, lastSeen, 100)
		if err != nil {
			return fmt.Errorf("poll request events: %w", err)
		}

		for _, event := range events {
			if event.Timestamp.After(lastSeen) {
				lastSeen = event.Timestamp
			}
			processed, err := a.bus.HasProcessed(ctx, plannerConsumerName, event.ID)
			if err != nil {
				return fmt.Errorf("check inbox for request event: %w", err)
			}
			if processed {
				a.logger.Info("request event already processed", "event_id", event.ID)
				continue
			}

			if err := a.handleRequestEvent(ctx, event); err != nil {
				a.logger.Error("failed to process request event", "error", err, "event_id", event.ID)
				continue
			}
			if err := a.bus.MarkProcessed(ctx, plannerConsumerName, event.ID); err != nil {
				return fmt.Errorf("mark request event processed: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *Agent) handleRequestEvent(ctx context.Context, event eventschema.Event) error {
	eventName, _ := event.Payload["event_name"].(string)
	if eventName != requestReceivedEventName {
		a.logger.Info("skipping unexpected event", "event_id", event.ID, "event_name", eventName, "topic", event.Topic)
		return nil
	}

	workflowID, _ := event.Payload["workflow_id"].(string)
	correlationID, _ := event.Payload["correlation_id"].(string)
	prompt, _ := event.Payload["prompt"].(string)
	if workflowID == "" || correlationID == "" || strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("request event missing required payload fields")
	}

	a.logger.Info("processing request event",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"payload", bus.PayloadKey(event.Payload),
	)

	response, err := a.ollamaClient.Chat(ctx, a.prompt, prompt)
	if err != nil {
		return fmt.Errorf("call ollama: %w", err)
	}

	plan, err := parsePlannerOutput(response)
	if err != nil {
		return fmt.Errorf("parse planner output: %w", err)
	}
	plan = ensureApprovalTask(plan, prompt)

	if err := a.bus.Publish(ctx, newEvent(planTopic, workflowID, correlationID, map[string]any{
		"event_name":     planCreatedEventName,
		"workflow_id":    workflowID,
		"correlation_id": correlationID,
		"intent":         plan.Intent,
		"tasks":          plan.Tasks,
	})); err != nil {
		return fmt.Errorf("publish plan.created: %w", err)
	}

	for _, task := range plan.Tasks {
		topic, eventName, err := taskPublication(task.Type)
		if err != nil {
			return err
		}

		payload := map[string]any{
			"event_name":     eventName,
			"workflow_id":    workflowID,
			"correlation_id": correlationID,
			"intent":         plan.Intent,
			"task_type":      task.Type,
			"reason":         task.Reason,
		}
		if task.Type == "retrieval" || task.Type == "classification" || task.Type == "response" || task.Type == "approval" {
			payload["prompt"] = prompt
		}

		if task.Type == "response" {
			a.logger.Info("deferring response task until prerequisite results are available",
				"workflow_id", workflowID,
				"correlation_id", correlationID,
			)
			continue
		}

		if err := a.bus.Publish(ctx, newEvent(topic, workflowID, correlationID, payload)); err != nil {
			return fmt.Errorf("publish %s: %w", eventName, err)
		}
	}

	a.logger.Info("request event processed",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"task_count", len(plan.Tasks),
		"intent", plan.Intent,
	)

	return nil
}

func parsePlannerOutput(raw string) (plannerOutput, error) {
	var output plannerOutput
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return plannerOutput{}, err
	}
	if decoder.More() {
		return plannerOutput{}, fmt.Errorf("planner output contained trailing data")
	}
	if strings.TrimSpace(output.Intent) == "" {
		return plannerOutput{}, fmt.Errorf("intent is required")
	}

	seen := make(map[string]struct{}, len(output.Tasks))
	for _, task := range output.Tasks {
		if strings.TrimSpace(task.Reason) == "" {
			return plannerOutput{}, fmt.Errorf("task reason is required")
		}
		if _, _, err := taskPublication(task.Type); err != nil {
			return plannerOutput{}, err
		}
		if _, ok := seen[task.Type]; ok {
			return plannerOutput{}, fmt.Errorf("duplicate task type %q", task.Type)
		}
		seen[task.Type] = struct{}{}
	}

	return output, nil
}

func taskPublication(taskType string) (topic string, eventName string, err error) {
	switch taskType {
	case "retrieval":
		return retrievalTopic, "task.retrieval.requested", nil
	case "classification":
		return classificationTopic, "task.classification.requested", nil
	case "approval":
		return approvalTopic, "approval.required", nil
	case "response":
		return responseTopic, "task.response.requested", nil
	default:
		return "", "", fmt.Errorf("unsupported task type %q", taskType)
	}
}

func ensureApprovalTask(plan plannerOutput, prompt string) plannerOutput {
	if !requiresApproval(prompt) {
		return plan
	}
	for _, task := range plan.Tasks {
		if task.Type == "approval" {
			return plan
		}
	}
	plan.Tasks = append(plan.Tasks, plannedTask{
		Type:   "approval",
		Reason: "operator approval is required before taking risky remediation steps",
	})
	return plan
}

func requiresApproval(prompt string) bool {
	lower := strings.ToLower(prompt)
	for _, marker := range []string{
		"rollback",
		"production",
		"prod ",
		"firewall",
		"shutdown",
		"disable",
		"delete",
		"restart",
		"change window",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func newEvent(topic, workflowID, correlationID string, payload map[string]any) eventschema.Event {
	return eventschema.Event{
		ID:        newID(),
		Topic:     topic,
		Status:    eventschema.EventStatusPending,
		Source:    "planner-agent",
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
