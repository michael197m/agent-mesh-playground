package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"agentmesh/services/aggregator-agent/internal/aggregator"
	"agentmesh/services/aggregator-agent/internal/bus"
	"agentmesh/services/aggregator-agent/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	dbPath := envOrDefault("AGGREGATOR_DB_PATH", "../api-gateway/api-gateway.db")
	pollInterval := 2 * time.Second
	workflowTimeout := envDurationOrDefault("AGGREGATOR_WORKFLOW_TIMEOUT", 45*time.Second)

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

	stateStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		logger.Error("failed to initialize state store", "error", err, "db_path", dbPath)
		os.Exit(1)
	}
	defer func() {
		if err := stateStore.Close(); err != nil {
			logger.Error("failed to close state store", "error", err)
		}
	}()

	agent, err := aggregator.NewAgent(aggregator.Config{
		PollInterval:    pollInterval,
		WorkflowTimeout: workflowTimeout,
		Logger:          logger,
		Bus:             messageBus,
		Store:           stateStore,
	})
	if err != nil {
		logger.Error("failed to initialize aggregator agent", "error", err)
		os.Exit(1)
	}

	logger.Info("aggregator agent started", "db_path", dbPath, "workflow_timeout", workflowTimeout)
	if err := agent.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("aggregator agent stopped with error", "error", err)
		os.Exit(1)
	}

	logger.Info("aggregator agent stopped")
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envDurationOrDefault(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return fallback
}
