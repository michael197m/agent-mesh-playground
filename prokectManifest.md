# Agent Mesh Playground Manifest

## Goal
Build a small local event-driven multi-agent system to understand agent mesh architecture.

This is NOT a chatbot.
This is a distributed workflow where specialized agents communicate through events.

## Core learning goals
- Understand the difference between a single-agent ReAct loop and a multi-agent mesh
- Learn event routing, fan-out, fan-in, correlation IDs, retries, idempotency, and partial failure handling
- See how a React UI can observe asynchronous agent workflows in real time

## Use case
Incident Triage Assistant

Example prompt:
"Our Toronto edge site is showing intermittent packet loss after a config update. Classify severity, retrieve relevant context, and recommend next steps."

## Architecture
Frontend:
- React + TypeScript
- Sends user request
- Shows workflow timeline and agent status

Backend:
- Go services
- Event-driven communication through a message bus abstraction
- Local Ollama integration for LLM-backed agents
- SQLite for workflow/event persistence and inbox dedupe

Agents:
1. planner-agent
2. retriever-agent
3. classifier-agent
4. responder-agent
5. aggregator-agent

Gateway:
- api-gateway

## Event-driven design
Every workflow starts with a single request event.
The planner emits task events.
Specialized agents process task events independently.
The aggregator waits for required completion events and emits the final workflow result.

## Non-goals
- No auth
- No multi-tenant security
- No production-grade deployment
- No embeddings in v1
- No external SaaS integrations

## Repo structure
agent-mesh-playground/
  README.md
  .env
  docker-compose.yml

  frontend/
    src/
      app/
      pages/
      components/
      hooks/
      lib/
      types/
    package.json
    vite.config.ts

  shared/
    event-schema/
      event.go
      event.ts
      topics.md
    prompts/
      planner.txt
      retriever.txt
      classifier.txt
      responder.txt
      aggregator.txt

  services/
    api-gateway/
      main.go
      handlers/
      bus/
      store/
      websocket/
    planner-agent/
      main.go
      prompt/
      bus/
      ollama/
    retriever-agent/
      main.go
      bus/
      store/
      ollama/
    classifier-agent/
      main.go
      bus/
      ollama/
    responder-agent/
      main.go
      bus/
      ollama/
    aggregator-agent/
      main.go
      bus/
      store/

  data/
    docs/
      runbook_1.md
      runbook_2.md
      incidents.json

  infra/
    scripts/

## Shared event envelope
Every event must use this structure:

{
  "event_id": "uuid",
  "correlation_id": "uuid",
  "causation_id": "uuid-or-null",
  "event_type": "string",
  "source": "string",
  "target": "string",
  "status": "requested|running|completed|failed",
  "timestamp": "RFC3339 string",
  "payload": {}
}

## Required event types
- request.received
- plan.created
- task.retrieval.requested
- task.classification.requested
- task.response.requested
- task.retrieval.completed
- task.classification.completed
- task.response.completed
- workflow.completed
- workflow.failed
- approval.required
- approval.received

## Subjects / topics
- mesh.request
- mesh.plan
- mesh.task.retrieval
- mesh.task.classification
- mesh.task.response
- mesh.result.retrieval
- mesh.result.classification
- mesh.result.response
- mesh.workflow.completed
- mesh.workflow.failed
- mesh.approval.required
- mesh.approval.received

## Service responsibilities

### api-gateway
- POST /api/chat accepts a user request
- Generates correlation_id
- Publishes request.received
- Exposes workflow/event stream to frontend
- Saves all events to SQLite

### planner-agent
- Consumes mesh.request
- Calls Ollama with planner prompt
- Produces plan.created
- Emits the required task events

### retriever-agent
- Consumes mesh.task.retrieval
- Searches local docs using keyword match
- Returns top snippets
- Produces task.retrieval.completed

### classifier-agent
- Consumes mesh.task.classification
- Calls Ollama to classify severity/category/confidence
- Produces task.classification.completed

### responder-agent
- Consumes mesh.task.response
- Calls Ollama to draft response
- Produces task.response.completed

### aggregator-agent
- Consumes all task completion events
- Stores partial results by correlation_id
- Emits workflow.completed when all required results exist
- Emits workflow.failed on timeout

## Persistence
Use SQLite for:
- workflows
- workflow_events
- workflow_state
- inbox

### workflows
id TEXT PRIMARY KEY
status TEXT
request_text TEXT
created_at TEXT
updated_at TEXT

### workflow_events
event_id TEXT PRIMARY KEY
correlation_id TEXT
event_type TEXT
source TEXT
target TEXT
status TEXT
payload_json TEXT
created_at TEXT

### workflow_state
correlation_id TEXT PRIMARY KEY
plan_json TEXT
retrieval_json TEXT
classification_json TEXT
response_json TEXT
approval_status TEXT
updated_at TEXT

### inbox
event_id TEXT PRIMARY KEY
consumer_name TEXT
processed_at TEXT

## Idempotency rules
- Every consumer must dedupe by event_id using the inbox table
- Reprocessing the same event must be safe
- Aggregator must tolerate duplicate completion events

## Failure rules
- retriever-agent fails randomly 20% of the time in v2
- retries use exponential backoff + jitter
- if required result never arrives within timeout, aggregator publishes workflow.failed

## UI requirements
The frontend must show:
1. request form
2. workflow timeline
3. agent status cards
4. final response panel
5. optional approve button for approval.required

## Build phases

### Phase 1
Single-agent baseline:
- frontend
- api-gateway
- direct Ollama call

### Phase 2
Event-driven baseline:
- api-gateway
- planner-agent
- responder-agent
- aggregator-agent

### Phase 3
Real mini mesh:
- retriever-agent
- classifier-agent
- fan-out and fan-in

### Phase 4
Resilience:
- retries
- idempotency
- timeout/failure flow

### Phase 5
Human-in-the-loop:
- approval.required
- approval.received

## Coding constraints
- Use clean package boundaries
- Reuse shared event envelope package
- Prefer explicit JSON contracts
- No hidden cross-service coupling
- No direct agent-to-agent HTTP calls in v1
- Communication between agents must be event-first

## Success criteria
The project is successful if:
- one user request triggers multiple agent tasks
- each task emits observable events
- the frontend shows the workflow evolving in real time
- duplicates are safe
- one failing agent does not crash the whole system
- final result is aggregated from multiple specialized agent outputs
