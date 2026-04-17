# MANIFEST.md

This repository implements a local event-driven multi-agent system called Agent Mesh Playground.

## Purpose
Learn agent mesh architecture by building a small distributed workflow where specialized agents communicate through events.

## Mandatory rules
- All inter-agent communication must be event-driven
- Use the shared event envelope for every published message
- Every workflow must have a correlation_id
- Every consumer must dedupe by event_id
- API gateway is the only frontend entrypoint
- No direct HTTP calls between agents
- Aggregator owns workflow completion logic

## Main services
- api-gateway
- planner-agent
- retriever-agent
- classifier-agent
- responder-agent
- aggregator-agent

## Main flow
1. frontend calls api-gateway
2. api-gateway publishes request.received
3. planner-agent emits task events
4. specialized agents process tasks
5. aggregator-agent waits for required completions
6. aggregator-agent emits workflow.completed
7. frontend renders the final response and event timeline

## Event topics
- mesh.request
- mesh.task.retrieval
- mesh.task.classification
- mesh.task.response
- mesh.result.retrieval
- mesh.result.classification
- mesh.result.response
- mesh.workflow.completed
- mesh.workflow.failed

## Persistence
SQLite tables:
- workflows
- workflow_events
- workflow_state
- inbox

## v1 scope
- Local only
- Ollama only
- Keyword retrieval only
- No auth
- No external integrations

## Priority order for implementation
1. shared event schema
2. api-gateway
3. planner-agent
4. responder-agent
5. aggregator-agent
6. retriever-agent
7. classifier-agent
8. frontend workflow timeline
9. retries + inbox dedupe
10. approval flow
