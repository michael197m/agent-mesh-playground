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

	"agentmesh/services/responder-agent/internal/bus"
	"agentmesh/services/responder-agent/internal/ollama"
	"agentmesh/services/responder-agent/internal/responder"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := envOrDefault("RESPONDER_DB_PATH", "../api-gateway/api-gateway.db")
	promptPath := envOrDefault("RESPONDER_PROMPT_PATH", "../../shared/prompts/responder.txt")
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
	agent, err := responder.NewAgent(responder.Config{
		PromptPath:   promptPath,
		PollInterval: pollInterval,
		Logger:       logger,
		Bus:          messageBus,
		OllamaClient: ollamaClient,
	})
	if err != nil {
		logger.Error("failed to initialize responder agent", "error", err)
		os.Exit(1)
	}

	logger.Info("responder agent started", "db_path", dbPath, "prompt_path", promptPath, "ollama_base_url", ollamaBaseURL, "ollama_model", ollamaModel)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("responder agent stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("responder agent stopped")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
