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

	"agentmesh/services/planner-agent/internal/bus"
	"agentmesh/services/planner-agent/internal/ollama"
	"agentmesh/services/planner-agent/internal/planner"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := envOrDefault("PLANNER_DB_PATH", "../api-gateway/api-gateway.db")
	promptPath := envOrDefault("PLANNER_PROMPT_PATH", "../../shared/prompts/planner.txt")
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
	agent, err := planner.NewAgent(planner.Config{
		PromptPath:   promptPath,
		PollInterval: pollInterval,
		Logger:       logger,
		Bus:          messageBus,
		OllamaClient: ollamaClient,
	})
	if err != nil {
		logger.Error("failed to initialize planner agent", "error", err)
		os.Exit(1)
	}

	logger.Info("planner agent started", "db_path", dbPath, "prompt_path", promptPath, "ollama_base_url", ollamaBaseURL, "ollama_model", ollamaModel)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("planner agent stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("planner agent stopped")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
