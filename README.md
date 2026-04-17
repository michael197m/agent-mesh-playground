# Agent Mesh Playground

Agent Mesh Playground is a local event-driven multi-agent workflow system.

It is built to show how a frontend, an API gateway, and multiple specialized backend agents can cooperate through events instead of direct service-to-service HTTP calls.

## What This Repo Does

The application accepts an operator-style request from a React frontend, starts a workflow in the API gateway, and then routes work across multiple Go services:

- `planner-agent` decides what sub-tasks are needed
- `retriever-agent` searches local docs
- `classifier-agent` classifies the request
- `aggregator-agent` coordinates workflow state and fan-in
- `responder-agent` generates the final synthesized response

All services communicate through events stored in a shared SQLite database.

## Architecture

## Sequence Diagram

```text
Frontend
  |
  | POST /api/chat
  v
API Gateway
  |
  | writes request.received -> mesh.request
  v
Planner Agent
  |
  | plan.created
  | task.retrieval.requested ------> Retriever Agent
  | task.classification.requested -> Classifier Agent
  |
  |<----- task.retrieval.completed ------ Retriever Agent
  |<-- task.classification.completed --- Classifier Agent
  v
Aggregator Agent
  |
  | task.response.requested
  v
Responder Agent
  |
  | task.response.completed
  v
Aggregator Agent
  |
  | workflow.completed or workflow.failed
  v
API Gateway event feed
  ^
  | GET /api/workflows/:workflowId/events
  |
Frontend
```

### Frontend

The frontend lives in `frontend/` and is a Vite + React app.

It:

- submits a request to the API gateway
- polls for workflow events
- renders workflow IDs
- shows agent status cards
- shows the latest workflow result
- shows a timeline of emitted events

Important files:

- [frontend/src/App.tsx](frontend/src/App.tsx)
- [frontend/src/lib/api.ts](frontend/src/lib/api.ts)
- [frontend/src/hooks/useWorkflowEvents.ts](frontend/src/hooks/useWorkflowEvents.ts)

### API Gateway

The API gateway lives in `services/api-gateway/`.

It is the only backend service the frontend talks to directly.

Its responsibilities are:

- accept `POST /api/chat`
- create a workflow record
- generate `workflow_id` and `correlation_id`
- emit the initial `request.received` event
- expose `GET /api/workflows/:workflowId/events` so the frontend can read workflow activity

Important files:

- [services/api-gateway/main.go](services/api-gateway/main.go)
- [services/api-gateway/internal/handlers/chat.go](services/api-gateway/internal/handlers/chat.go)
- [services/api-gateway/internal/store/store.go](services/api-gateway/internal/store/store.go)

### Shared Event Bus

There is no external message broker in this project.

Instead, the system uses SQLite as a local event log and message bus:

- events are stored in the `workflow_events` table
- workflows are stored in the `workflows` table
- workflow state is stored in the `workflow_state` table
- event deduplication uses the `inbox` table

Each backend agent polls SQLite for the topics it cares about, publishes new events by inserting rows, and records which events it has already processed.

Shared event code:

- [shared/event-schema/event.go](shared/event-schema/event.go)
- [shared/event-schema/inbox/inbox.go](shared/event-schema/inbox/inbox.go)

### Agents

#### Planner Agent

The planner listens for `mesh.request`.

It calls Ollama with the planner prompt, produces a plan, and emits:

- `mesh.plan`
- `mesh.task.retrieval`
- `mesh.task.classification`

The planner no longer emits the response task immediately. Response generation is deferred until prerequisite results exist.

Important file:

- [services/planner-agent/internal/planner/agent.go](services/planner-agent/internal/planner/agent.go)

#### Retriever Agent

The retriever listens for `mesh.task.retrieval`.

It searches local docs with keyword matching and publishes:

- `mesh.result.retrieval`

Its payload includes:

- `summary`
- `snippets`
- `retrieval_context`

Important file:

- [services/retriever-agent/internal/retriever/agent.go](services/retriever-agent/internal/retriever/agent.go)

#### Classifier Agent

The classifier listens for `mesh.task.classification`.

It calls Ollama and publishes:

- `mesh.result.classification`

Its payload includes:

- `severity`
- `category`
- `confidence`
- `reason`
- `classification_result`

Important file:

- [services/classifier-agent/internal/classifier/agent.go](services/classifier-agent/internal/classifier/agent.go)

#### Aggregator Agent

The aggregator is the workflow coordinator.

It listens for:

- `mesh.result.retrieval`
- `mesh.result.classification`
- `mesh.result.response`

It persists workflow state, requests the response task once retrieval and classification are both available, and marks workflows terminal:

- emits `mesh.task.response` after fan-in prerequisites are met
- emits `mesh.workflow.completed` when all required results exist
- emits `mesh.workflow.failed` when the workflow times out

Important files:

- [services/aggregator-agent/internal/aggregator/agent.go](services/aggregator-agent/internal/aggregator/agent.go)
- [services/aggregator-agent/internal/store/store.go](services/aggregator-agent/internal/store/store.go)

#### Responder Agent

The responder listens for `mesh.task.response`.

It calls Ollama using:

- the original request
- `retrieval_context`
- `classification_result`

It then publishes:

- `mesh.result.response`

Its payload includes:

- `response`
- `recommended_actions`

Important file:

- [services/responder-agent/internal/responder/agent.go](services/responder-agent/internal/responder/agent.go)

## How Ollama Is Used

Ollama is the local LLM provider for structured task execution.

The following services call Ollama:

- `planner-agent`
- `classifier-agent`
- `responder-agent`
- optionally `retriever-agent` if Ollama summarization is enabled

Each of those services:

1. loads a system prompt from `shared/prompts/`
2. constructs a user prompt from the current event payload
3. sends a non-streaming JSON-formatted chat request to Ollama
4. parses the JSON response into a typed result

The model defaults to `llama3.1`, and the base URL defaults to `http://localhost:11434`.

## End-to-End Flow

1. The user enters a request in the frontend.
2. The frontend sends `POST /api/chat` to the API gateway.
3. The API gateway creates a workflow row and writes `request.received` to `mesh.request`.
4. The planner consumes `mesh.request`, calls Ollama, and emits:
   - `plan.created`
   - `task.retrieval.requested`
   - `task.classification.requested`
5. The retriever consumes `mesh.task.retrieval` and publishes `task.retrieval.completed`.
6. The classifier consumes `mesh.task.classification` and publishes `task.classification.completed`.
7. The aggregator stores both partial results.
8. Once retrieval and classification both exist, the aggregator emits `task.response.requested`.
9. The responder consumes `mesh.task.response`, calls Ollama with the original prompt plus retrieval/classification context, and publishes `task.response.completed`.
10. The aggregator sees all required results and emits `workflow.completed`.
11. If required results do not arrive in time, the aggregator emits `workflow.failed`.
12. The frontend polls the workflow event feed and renders status, output, and timeline.

## Event Topics

Main topics in the current application:

- `mesh.request`
- `mesh.plan`
- `mesh.task.retrieval`
- `mesh.task.classification`
- `mesh.task.response`
- `mesh.result.retrieval`
- `mesh.result.classification`
- `mesh.result.response`
- `mesh.workflow.completed`
- `mesh.workflow.failed`

## Running The Project

### Prerequisites

You need:

- Go
- Node.js and npm
- Ollama

### 1. Start Ollama

In one terminal:

```bash
ollama serve
```

If the model is not already installed:

```bash
ollama pull llama3.1
```

### 2. Start The Backend

From the repo root:

```bash
cd /home/michael/repos/agent-mesh-playground
./scripts/run-backend.sh
```

If port `8080` is already in use:

```bash
API_GATEWAY_ADDR=:8081 ./scripts/run-backend.sh
```

What the backend runner does:

- starts all backend services
- uses a shared SQLite database under `runtime/mesh.db`
- writes service logs to `runtime/logs/`
- cleans up processes it started on shutdown

Useful environment overrides:

```bash
API_GATEWAY_ADDR=:8081 \
OLLAMA_BASE_URL=http://localhost:11434 \
OLLAMA_MODEL=llama3.1 \
AGGREGATOR_WORKFLOW_TIMEOUT=45s \
./scripts/run-backend.sh
```

### 3. Start The Frontend

In another terminal:

```bash
cd /home/michael/repos/agent-mesh-playground/frontend
npm run dev
```

If the backend is running on `8081` instead of `8080`:

```bash
VITE_API_BASE_URL=http://localhost:8081 npm run dev
```

Then open the local Vite URL shown in the terminal, typically:

```text
http://localhost:5173
```

### 4. Submit A Workflow

Use the frontend request form and submit an operator-style prompt, for example:

```text
Our Toronto edge site is showing intermittent packet loss after a config update. Classify severity, retrieve relevant context, and recommend next steps.
```

You should see:

- workflow identifiers created in the left panel
- status cards move through the workflow
- the timeline fill with raw events
- the latest result panel show either:
  - a completed response
  - or a failed workflow with the reason

## Logs And Debugging

Backend logs are written to:

```text
runtime/logs/
```

Useful files:

- `runtime/logs/api-gateway.log`
- `runtime/logs/planner-agent.log`
- `runtime/logs/retriever-agent.log`
- `runtime/logs/classifier-agent.log`
- `runtime/logs/responder-agent.log`
- `runtime/logs/aggregator-agent.log`

To inspect the latest lines:

```bash
tail -n 50 runtime/logs/api-gateway.log
tail -n 50 runtime/logs/planner-agent.log
tail -n 50 runtime/logs/aggregator-agent.log
```

## Current Limitations

The current application works as a local event-driven workflow, but there are still known gaps:

- the frontend reconstructs state from raw events instead of a normalized workflow summary endpoint
- transport is polling-based rather than SSE/WebSocket
- LLM output parsing is still strict and brittle
- there is no retry/backoff strategy for failed tasks
- end-to-end integration tests are still limited

## Relevant Files

- [MANIFEST.md](MANIFEST.md)
- [services/api-gateway/main.go](services/api-gateway/main.go)
- [services/planner-agent/main.go](services/planner-agent/main.go)
- [services/retriever-agent/main.go](services/retriever-agent/main.go)
- [services/classifier-agent/main.go](services/classifier-agent/main.go)
- [services/responder-agent/main.go](services/responder-agent/main.go)
- [services/aggregator-agent/main.go](services/aggregator-agent/main.go)
- [scripts/run-backend.sh](scripts/run-backend.sh)
