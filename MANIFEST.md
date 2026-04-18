# MANIFEST.md

This file captures the current project contract at a glance.

`README.md` is the authoritative document for architecture, runtime setup, and workflow behavior.

## Purpose
Build and study a local event-driven multi-agent workflow where specialized services coordinate through persisted events.

## Repository Contract
- All inter-agent communication is event-driven through the shared SQLite event log.
- The API gateway is the only service the frontend talks to directly.
- Every workflow has both a `workflow_id` and a `correlation_id`.
- Every consumer deduplicates by `event_id` using the inbox table.
- The aggregator owns response fan-in, workflow completion, and workflow failure.

## Active Services
- `api-gateway`
- `planner-agent`
- `retriever-agent`
- `classifier-agent`
- `responder-agent`
- `aggregator-agent`

## Active Topics
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

## Persistence
SQLite tables currently used by the system:
- `workflows`
- `workflow_events`
- `workflow_state`
- `inbox`

## Scope
- Local-only runtime
- Ollama-backed planner, classifier, and responder
- Keyword-based retrieval with optional Ollama summarization
- No auth
- No external integrations
