package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"agentmesh/services/retriever-agent/internal/bus"
	"agentmesh/services/retriever-agent/internal/ollama"
	"agentmesh/services/retriever-agent/internal/retriever"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := envOrDefault("RETRIEVER_DB_PATH", "../api-gateway/api-gateway.db")
	docsPath := envOrDefault("RETRIEVER_DOCS_PATH", "../../data/docs")
	promptPath := envOrDefault("RETRIEVER_PROMPT_PATH", "../../shared/prompts/retriever.txt")
	ollamaBaseURL := envOrDefault("OLLAMA_BASE_URL", "http://localhost:11434")
	ollamaModel := envOrDefault("OLLAMA_MODEL", "llama3.1")
	pollInterval := 2 * time.Second
	useOllamaSummary := envBool("RETRIEVER_USE_OLLAMA_SUMMARY", false)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	messageBus, err := bus.NewSQLiteBus(dbPath, logger)
	if err != nil {
		logger.Error("failed to initialize bus", "error", err, "db_path", dbPath)
		os.Exit(1)
	}
	defer func() {
		if err := messageBus.Close(); err != nil {
			logger.Error("failed to close bus", "error", err)
		}
	}()

	var ollamaClient retriever.OllamaClient
	if useOllamaSummary {
		ollamaClient = ollama.NewClient(ollamaBaseURL, ollamaModel, http.DefaultClient)
	}

	agent, err := retriever.NewAgent(retriever.Config{
		PromptPath:       promptPath,
		DocsPath:         docsPath,
		PollInterval:     pollInterval,
		UseOllamaSummary: useOllamaSummary,
		Logger:           logger,
		Bus:              messageBus,
		OllamaClient:     ollamaClient,
	})
	if err != nil {
		logger.Error("failed to initialize retriever agent", "error", err)
		os.Exit(1)
	}

	logger.Info("retriever agent started", "db_path", dbPath, "docs_path", docsPath, "prompt_path", promptPath, "use_ollama_summary", useOllamaSummary)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("retriever agent stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("retriever agent stopped")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
