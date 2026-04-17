package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agentmesh/services/classifier-agent/internal/bus"
	"agentmesh/services/classifier-agent/internal/classifier"
	"agentmesh/services/classifier-agent/internal/ollama"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := envOrDefault("CLASSIFIER_DB_PATH", "../api-gateway/api-gateway.db")
	promptPath := envOrDefault("CLASSIFIER_PROMPT_PATH", "../../shared/prompts/classifier.txt")
	ollamaBaseURL := envOrDefault("OLLAMA_BASE_URL", "http://localhost:11434")
	ollamaModel := envOrDefault("OLLAMA_MODEL", "llama3.1")
	pollInterval := 2 * time.Second

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

	ollamaClient := ollama.NewClient(ollamaBaseURL, ollamaModel, http.DefaultClient)
	agent, err := classifier.NewAgent(classifier.Config{
		PromptPath:   promptPath,
		PollInterval: pollInterval,
		Logger:       logger,
		Bus:          messageBus,
		OllamaClient: ollamaClient,
	})
	if err != nil {
		logger.Error("failed to initialize classifier agent", "error", err)
		os.Exit(1)
	}

	logger.Info("classifier agent started", "db_path", dbPath, "prompt_path", promptPath, "ollama_base_url", ollamaBaseURL, "ollama_model", ollamaModel)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("classifier agent stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("classifier agent stopped")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
