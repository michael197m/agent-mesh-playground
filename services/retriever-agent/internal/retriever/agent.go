package retriever

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	"agentmesh/services/retriever-agent/internal/bus"
	eventschema "agentmesh/shared/event-schema"
)

const (
	retrieverConsumerName       = "retriever-agent"
	retrievalTaskTopic          = "mesh.task.retrieval"
	retrievalResultTopic        = "mesh.result.retrieval"
	retrievalRequestedEventName = "task.retrieval.requested"
	retrievalCompletedEventName = "task.retrieval.completed"
	maxResults                  = 5
	maxExcerptLength            = 220
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
	PromptPath       string
	DocsPath         string
	PollInterval     time.Duration
	UseOllamaSummary bool
	Logger           *slog.Logger
	Bus              Bus
	OllamaClient     OllamaClient
}

type Agent struct {
	prompt           string
	docsPath         string
	pollInterval     time.Duration
	useOllamaSummary bool
	logger           *slog.Logger
	bus              Bus
	ollamaClient     OllamaClient
}

type snippet struct {
	Title   string `json:"title"`
	Excerpt string `json:"excerpt"`
}

type retrievalOutput struct {
	Summary  string    `json:"summary"`
	Snippets []snippet `json:"snippets"`
}

type rankedSnippet struct {
	snippet
	score int
}

func NewAgent(cfg Config) (*Agent, error) {
	if cfg.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}
	if cfg.Bus == nil {
		return nil, fmt.Errorf("bus is required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 2 * time.Second
	}
	if cfg.DocsPath == "" {
		return nil, fmt.Errorf("docs path is required")
	}
	if cfg.UseOllamaSummary {
		if cfg.OllamaClient == nil {
			return nil, fmt.Errorf("ollama client is required when ollama summary is enabled")
		}
		promptBytes, err := os.ReadFile(cfg.PromptPath)
		if err != nil {
			return nil, fmt.Errorf("read retriever prompt: %w", err)
		}
		return &Agent{
			prompt:           string(promptBytes),
			docsPath:         cfg.DocsPath,
			pollInterval:     cfg.PollInterval,
			useOllamaSummary: true,
			logger:           cfg.Logger,
			bus:              cfg.Bus,
			ollamaClient:     cfg.OllamaClient,
		}, nil
	}

	return &Agent{
		docsPath:         cfg.DocsPath,
		pollInterval:     cfg.PollInterval,
		useOllamaSummary: false,
		logger:           cfg.Logger,
		bus:              cfg.Bus,
	}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	lastSeen := time.Time{}
	ticker := time.NewTicker(a.pollInterval)
	defer ticker.Stop()

	for {
		events, err := a.bus.Poll(ctx, retrievalTaskTopic, lastSeen, 100)
		if err != nil {
			return fmt.Errorf("poll retrieval task events: %w", err)
		}

		for _, event := range events {
			if event.Timestamp.After(lastSeen) {
				lastSeen = event.Timestamp
			}
			processed, err := a.bus.HasProcessed(ctx, retrieverConsumerName, event.ID)
			if err != nil {
				return fmt.Errorf("check inbox for retrieval task: %w", err)
			}
			if processed {
				a.logger.Info("retrieval task already processed", "event_id", event.ID)
				continue
			}

			if err := a.handleRetrievalTask(ctx, event); err != nil {
				a.logger.Error("failed to process retrieval task", "error", err, "event_id", event.ID)
				continue
			}
			if err := a.bus.MarkProcessed(ctx, retrieverConsumerName, event.ID); err != nil {
				return fmt.Errorf("mark retrieval task processed: %w", err)
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (a *Agent) handleRetrievalTask(ctx context.Context, event eventschema.Event) error {
	eventName, _ := event.Payload["event_name"].(string)
	if eventName != retrievalRequestedEventName {
		a.logger.Info("skipping unexpected event", "event_id", event.ID, "event_name", eventName, "topic", event.Topic)
		return nil
	}

	workflowID, _ := event.Payload["workflow_id"].(string)
	correlationID, _ := event.Payload["correlation_id"].(string)
	prompt, _ := event.Payload["prompt"].(string)
	reason, _ := event.Payload["reason"].(string)
	if workflowID == "" || correlationID == "" || strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("retrieval task missing required payload fields")
	}

	a.logger.Info("processing retrieval task",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"payload", bus.PayloadKey(event.Payload),
	)

	snippets, err := a.searchSnippets(prompt)
	if err != nil {
		return fmt.Errorf("search snippets: %w", err)
	}

	summary := deterministicSummary(prompt, snippets)
	if a.useOllamaSummary {
		userPrompt := buildSummarizationInput(prompt, snippets)
		raw, err := a.ollamaClient.Chat(ctx, a.prompt, userPrompt)
		if err != nil {
			return fmt.Errorf("call ollama: %w", err)
		}
		parsed, err := parseRetrieverOutput(raw)
		if err != nil {
			return fmt.Errorf("parse retriever output: %w", err)
		}
		summary = parsed.Summary
		if len(parsed.Snippets) > 0 {
			snippets = parsed.Snippets
		}
	}

	resultEvent := eventschema.Event{
		ID:        newID(),
		Topic:     retrievalResultTopic,
		Status:    eventschema.EventStatusSucceeded,
		Source:    "retriever-agent",
		Timestamp: time.Now().UTC(),
		Payload: map[string]any{
			"event_name":        retrievalCompletedEventName,
			"workflow_id":       workflowID,
			"correlation_id":    correlationID,
			"task_type":         "retrieval",
			"summary":           summary,
			"snippets":          snippets,
			"retrieval_context": summary,
		},
	}
	if reason != "" {
		resultEvent.Payload["reason"] = reason
	}
	if intent, ok := event.Payload["intent"].(string); ok && intent != "" {
		resultEvent.Payload["intent"] = intent
	}

	if err := a.bus.Publish(ctx, resultEvent); err != nil {
		return fmt.Errorf("publish %s: %w", retrievalCompletedEventName, err)
	}

	a.logger.Info("retrieval task processed",
		"event_id", event.ID,
		"workflow_id", workflowID,
		"correlation_id", correlationID,
		"snippet_count", len(snippets),
	)

	return nil
}

func (a *Agent) searchSnippets(query string) ([]snippet, error) {
	terms := keywords(query)
	if len(terms) == 0 {
		return nil, nil
	}

	ranked := make([]rankedSnippet, 0)
	err := filepath.WalkDir(a.docsPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}

		matches, err := fileMatches(path, terms)
		if err != nil {
			return err
		}
		ranked = append(ranked, matches...)
		return nil
	})
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score == ranked[j].score {
			if ranked[i].Title == ranked[j].Title {
				return ranked[i].Excerpt < ranked[j].Excerpt
			}
			return ranked[i].Title < ranked[j].Title
		}
		return ranked[i].score > ranked[j].score
	})

	if len(ranked) > maxResults {
		ranked = ranked[:maxResults]
	}

	results := make([]snippet, 0, len(ranked))
	for _, item := range ranked {
		results = append(results, item.snippet)
	}
	return results, nil
}

func fileMatches(path string, terms []string) ([]rankedSnippet, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	relTitle := path
	scanner := bufio.NewScanner(file)
	lineNumber := 0
	matches := make([]rankedSnippet, 0)
	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()
		lower := strings.ToLower(line)
		score := 0
		for _, term := range terms {
			if strings.Contains(lower, term) {
				score++
			}
		}
		if score == 0 {
			continue
		}

		matches = append(matches, rankedSnippet{
			snippet: snippet{
				Title:   fmt.Sprintf("%s:%d", relTitle, lineNumber),
				Excerpt: compactExcerpt(line),
			},
			score: score,
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return matches, nil
}

func parseRetrieverOutput(raw string) (retrievalOutput, error) {
	var output retrievalOutput
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&output); err != nil {
		return retrievalOutput{}, err
	}
	if decoder.More() {
		return retrievalOutput{}, fmt.Errorf("retriever output contained trailing data")
	}
	if strings.TrimSpace(output.Summary) == "" {
		return retrievalOutput{}, fmt.Errorf("summary is required")
	}
	for _, item := range output.Snippets {
		if strings.TrimSpace(item.Title) == "" || strings.TrimSpace(item.Excerpt) == "" {
			return retrievalOutput{}, fmt.Errorf("snippet title and excerpt are required")
		}
	}
	return output, nil
}

func buildSummarizationInput(prompt string, snippets []snippet) string {
	parts := []string{"user_request:\n" + prompt, "matched_snippets:"}
	if len(snippets) == 0 {
		parts = append(parts, "[]")
		return strings.Join(parts, "\n\n")
	}
	for _, item := range snippets {
		parts = append(parts, fmt.Sprintf("- title: %s\n  excerpt: %s", item.Title, item.Excerpt))
	}
	return strings.Join(parts, "\n\n")
}

func deterministicSummary(prompt string, snippets []snippet) string {
	if len(snippets) == 0 {
		return fmt.Sprintf("No matching local documents found for: %s", strings.TrimSpace(prompt))
	}
	return fmt.Sprintf("Found %d matching snippet(s) for: %s", len(snippets), strings.TrimSpace(prompt))
}

func compactExcerpt(line string) string {
	line = strings.Join(strings.Fields(line), " ")
	if len(line) <= maxExcerptLength {
		return line
	}
	return strings.TrimSpace(line[:maxExcerptLength-3]) + "..."
}

func keywords(query string) []string {
	fields := strings.FieldsFunc(strings.ToLower(query), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	seen := make(map[string]struct{}, len(fields))
	terms := make([]string, 0, len(fields))
	for _, field := range fields {
		if len(field) < 3 {
			continue
		}
		if _, ok := seen[field]; ok {
			continue
		}
		seen[field] = struct{}{}
		terms = append(terms, field)
	}
	sort.Strings(terms)
	return terms
}

func newID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
