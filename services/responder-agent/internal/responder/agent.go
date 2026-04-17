package responder

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

	"agentmesh/services/responder-agent/internal/bus"
	eventschema "agentmesh/shared/event-schema"
)

const (
	responderConsumerName      = "responder-agent"
	responseTaskTopic          = "mesh.task.response"
	responseResultTopic        = "mesh.result.response"
	responseRequestedEventName = "task.response.requested"
	responseCompletedEventName = "task.response.completed"
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

type responseOutput struct {
	Response           string   `json:"response"`
	RecommendedActions []string `json:"recommended_actions"`
}

func NewAgent(cfg Config) (*Agent, error) {
	prompt, err := os.ReadFile(cfg.PromptPath)
	if err != nil {
		return nil, fmt.Errorf("read responder prompt: %w", err)
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
		events, err := a.bus.Poll(ctx, responseTaskTopic, lastSeen, 100)
		if err != nil {
			return fmt.Errorf("poll response task events: %w", err)
		}

		for _, event := range events {
			if event.Timestamp.After(lastSeen) {
				lastSeen = event.Timestamp
			}
			processed, err := a.bus.HasProcessed(ctx, responderConsumerName, event.ID)
			if err != nil {
				return fmt.Errorf("check inbox for response task: %w", err)
			}
			if processed {
				a.logger.Info("response task already processed", "event_id", event.ID)
				continue
			}

			if err := a.handleResponseTask(ctx, event); err != nil {
				a.logger.Error("failed to process response task", "error", err, "event_id", event.ID)
				continue
			}
			if err := a.bus.MarkProcessed(ctx, responderConsumerName, event.ID); err != nil {
				return fmt.Errorf("mark response task processed: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *Agent) handleResponseTask(ctx context.Context, event eventschema.Event) error {
	eventName, _ := event.Payload["event_name"].(string)
	if eventName != responseRequestedEventName {
		a.logger.Info("skipping unexpected event", "event_id", event.ID, "event_name", eventName, "topic", event.Topic)
		return nil
	}

	workflowID, _ := event.Payload["workflow_id"].(string)
	correlationID, _ := event.Payload["correlation_id"].(string)
	prompt, _ := event.Payload["prompt"].(string)
	if workflowID == "" || correlationID == "" || strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("response task missing required payload fields")
	}

	a.logger.Info("processing response task",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"payload", bus.PayloadKey(event.Payload),
	)

	userPrompt := buildResponderInput(event.Payload)
	response, err := a.ollamaClient.Chat(ctx, a.prompt, userPrompt)
	if err != nil {
		return fmt.Errorf("call ollama: %w", err)
	}

	parsed, err := parseResponseOutput(response)
	if err != nil {
		return fmt.Errorf("parse responder output: %w", err)
	}

	resultEvent := eventschema.Event{
		ID:        newID(),
		Topic:     responseResultTopic,
		Status:    eventschema.EventStatusSucceeded,
		Source:    "responder-agent",
		Timestamp: time.Now().UTC(),
		Payload: map[string]any{
			"event_name":          responseCompletedEventName,
			"workflow_id":         workflowID,
			"correlation_id":      correlationID,
			"task_type":           "response",
			"response":            parsed.Response,
			"recommended_actions": parsed.RecommendedActions,
		},
	}
	if intent, ok := event.Payload["intent"].(string); ok && intent != "" {
		resultEvent.Payload["intent"] = intent
	}

	if err := a.bus.Publish(ctx, resultEvent); err != nil {
		return fmt.Errorf("publish %s: %w", responseCompletedEventName, err)
	}

	a.logger.Info("response task processed",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"recommended_action_count", len(parsed.RecommendedActions),
	)

	return nil
}

func parseResponseOutput(raw string) (responseOutput, error) {
	var output responseOutput
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return responseOutput{}, err
	}
	if decoder.More() {
		return responseOutput{}, fmt.Errorf("responder output contained trailing data")
	}
	if strings.TrimSpace(output.Response) == "" {
		return responseOutput{}, fmt.Errorf("response is required")
	}
	for _, action := range output.RecommendedActions {
		if strings.TrimSpace(action) == "" {
			return responseOutput{}, fmt.Errorf("recommended actions must not be empty")
		}
	}

	return output, nil
}

func buildResponderInput(payload map[string]any) string {
	sections := []string{}
	if prompt, ok := payload["prompt"].(string); ok && strings.TrimSpace(prompt) != "" {
		sections = append(sections, "original_user_request:\n"+prompt)
	}
	if retrievalContext, ok := payload["retrieval_context"].(string); ok && strings.TrimSpace(retrievalContext) != "" {
		sections = append(sections, "retrieval_context:\n"+retrievalContext)
	}
	if classificationResult, ok := payload["classification_result"].(string); ok && strings.TrimSpace(classificationResult) != "" {
		sections = append(sections, "classification_result:\n"+classificationResult)
	}
	if len(sections) == 0 {
		return "{}"
	}
	return strings.Join(sections, "\n\n")
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
