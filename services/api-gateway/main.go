package main

import (
	"log"
	"net/http"
	"os"

	"agentmesh/services/api-gateway/internal/bus"
	"agentmesh/services/api-gateway/internal/handlers"
	"agentmesh/services/api-gateway/internal/store"
)

func main() {
	logger := log.New(os.Stdout, "api-gateway ", log.LstdFlags|log.LUTC)

	dbPath := os.Getenv("API_GATEWAY_DB_PATH")
	if dbPath == "" {
		dbPath = "api-gateway.db"
	}

	workflowStore, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		if err := workflowStore.Close(); err != nil {
			logger.Printf("close database: %v", err)
		}
	}()

	publisher, err := bus.NewSQLitePublisher(dbPath, logger)
	if err != nil {
		logger.Fatal(err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Printf("close publisher database: %v", err)
		}
	}()

	chatHandler := handlers.NewChatHandler(publisher, workflowStore)
	workflowHandler := handlers.NewWorkflowHandler(publisher, workflowStore)

	mux := http.NewServeMux()
	mux.Handle("/api/chat", chatHandler)
	mux.Handle("/api/workflows/", workflowHandler)

	addr := os.Getenv("API_GATEWAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	logger.Printf("listening on %s with db=%s", addr, dbPath)
	if err := http.ListenAndServe(addr, withCORS(mux)); err != nil {
		logger.Fatal(err)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
