package classifier

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	"agentmesh/services/classifier-agent/internal/bus"
	eventschema "agentmesh/shared/event-schema"
)

const (
	classifierConsumerName           = "classifier-agent"
	classificationTaskTopic          = "mesh.task.classification"
	classificationResultTopic        = "mesh.result.classification"
	classificationRequestedEventName = "task.classification.requested"
	classificationCompletedEventName = "task.classification.completed"
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

type classificationOutput struct {
	Severity   string  `json:"severity"`
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	Reason     string  `json:"reason"`
}

func NewAgent(cfg Config) (*Agent, error) {
	prompt, err := os.ReadFile(cfg.PromptPath)
	if err != nil {
		return nil, fmt.Errorf("read classifier prompt: %w", err)
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
		events, err := a.bus.Poll(ctx, classificationTaskTopic, lastSeen, 100)
		if err != nil {
			return fmt.Errorf("poll classification task events: %w", err)
		}

		for _, event := range events {
			if event.Timestamp.After(lastSeen) {
				lastSeen = event.Timestamp
			}
			processed, err := a.bus.HasProcessed(ctx, classifierConsumerName, event.ID)
			if err != nil {
				return fmt.Errorf("check inbox for classification task: %w", err)
			}
			if processed {
				a.logger.Info("classification task already processed", "event_id", event.ID)
				continue
			}

			if err := a.handleClassificationTask(ctx, event); err != nil {
				a.logger.Error("failed to process classification task", "error", err, "event_id", event.ID)
				continue
			}
			if err := a.bus.MarkProcessed(ctx, classifierConsumerName, event.ID); err != nil {
				return fmt.Errorf("mark classification task processed: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *Agent) handleClassificationTask(ctx context.Context, event eventschema.Event) error {
	eventName, _ := event.Payload["event_name"].(string)
	if eventName != classificationRequestedEventName {
		a.logger.Info("skipping unexpected event", "event_id", event.ID, "event_name", eventName, "topic", event.Topic)
		return nil
	}

	workflowID, _ := event.Payload["workflow_id"].(string)
	correlationID, _ := event.Payload["correlation_id"].(string)
	prompt, _ := event.Payload["prompt"].(string)
	if workflowID == "" || correlationID == "" || strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("classification task missing required payload fields")
	}

	a.logger.Info("processing classification task",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"payload", bus.PayloadKey(event.Payload),
	)

	raw, err := a.ollamaClient.Chat(ctx, a.prompt, prompt)
	if err != nil {
		return fmt.Errorf("call ollama: %w", err)
	}

	parsed, err := parseClassificationOutput(raw)
	if err != nil {
		return fmt.Errorf("parse classifier output: %w", err)
	}

	resultEvent := eventschema.Event{
		ID:        newID(),
		Topic:     classificationResultTopic,
		Status:    eventschema.EventStatusSucceeded,
		Source:    "classifier-agent",
		Timestamp: time.Now().UTC(),
		Payload: map[string]any{
			"event_name":            classificationCompletedEventName,
			"workflow_id":           workflowID,
			"correlation_id":        correlationID,
			"task_type":             "classification",
			"severity":              parsed.Severity,
			"category":              parsed.Category,
			"confidence":            parsed.Confidence,
			"reason":                parsed.Reason,
			"classification_result": formatClassificationResult(parsed),
		},
	}
	if intent, ok := event.Payload["intent"].(string); ok && intent != "" {
		resultEvent.Payload["intent"] = intent
	}

	if err := a.bus.Publish(ctx, resultEvent); err != nil {
		return fmt.Errorf("publish %s: %w", classificationCompletedEventName, err)
	}

	a.logger.Info("classification task processed",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"severity", parsed.Severity,
		"category", parsed.Category,
		"confidence", parsed.Confidence,
	)

	return nil
}

func parseClassificationOutput(raw string) (classificationOutput, error) {
	var output classificationOutput
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return classificationOutput{}, err
	}
	if decoder.More() {
		return classificationOutput{}, fmt.Errorf("classifier output contained trailing data")
	}
	if strings.TrimSpace(output.Reason) == "" {
		return classificationOutput{}, fmt.Errorf("reason is required")
	}
	if !isSeverity(output.Severity) {
		return classificationOutput{}, fmt.Errorf("unsupported severity %q", output.Severity)
	}
	if !isCategory(output.Category) {
		return classificationOutput{}, fmt.Errorf("unsupported category %q", output.Category)
	}
	if math.IsNaN(output.Confidence) || math.IsInf(output.Confidence, 0) {
		return classificationOutput{}, fmt.Errorf("confidence must be a finite number")
	}
	if output.Confidence < 0 || output.Confidence > 1 {
		return classificationOutput{}, fmt.Errorf("confidence must be between 0 and 1")
	}

	return output, nil
}

func formatClassificationResult(output classificationOutput) string {
	return fmt.Sprintf("severity=%s; category=%s; confidence=%.2f; reason=%s", output.Severity, output.Category, output.Confidence, output.Reason)
}

func isSeverity(value string) bool {
	switch value {
	case "low", "medium", "high", "critical":
		return true
	default:
		return false
	}
}

func isCategory(value string) bool {
	switch value {
	case "network-config", "performance", "incident", "unknown":
		return true
	default:
		return false
	}
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
