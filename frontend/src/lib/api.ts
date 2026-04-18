import type { Event } from "../../../shared/event-schema/event";

export interface ChatRequest {
  prompt: string;
}

export interface ApprovalRequest {
  decision: "approved" | "rejected";
  comment?: string;
}

export interface ChatResponse {
  workflow_id: string;
  correlation_id: string;
}

export interface WorkflowEventsResponse {
  events: Event[];
}

export interface WorkflowSummary {
  workflow_id: string;
  correlation_id: string;
  prompt: string;
  status: string;
  created_at: string;
  updated_at: string;
  event_count: number;
  intent?: string;
  approval_status?: string;
  approval_decision?: string;
  approval_comment?: string;
  failure_reason?: string;
  retrieval_result?: Record<string, unknown>;
  classification_result?: Record<string, unknown>;
  response_result?: Record<string, unknown>;
  latest_event?: Event;
}

export interface WorkflowHistoryResponse {
  workflows: WorkflowSummary[];
}

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

export async function submitChat(request: ChatRequest): Promise<ChatResponse> {
  const response = await fetch(`${API_BASE_URL}/api/chat`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(`chat request failed with status ${response.status}`);
  }

  return (await response.json()) as ChatResponse;
}

export async function fetchWorkflowEvents(workflowId: string): Promise<Event[]> {
  const response = await fetch(`${API_BASE_URL}/api/workflows/${workflowId}/events`);

  if (response.status === 404) {
    return [];
  }

  if (!response.ok) {
    throw new Error(`event fetch failed with status ${response.status}`);
  }

  const data = (await response.json()) as WorkflowEventsResponse;
  return data.events;
}

export async function fetchWorkflowSummary(workflowId: string): Promise<WorkflowSummary> {
  const response = await fetch(`${API_BASE_URL}/api/workflows/${workflowId}`);

  if (!response.ok) {
    throw new Error(`workflow summary fetch failed with status ${response.status}`);
  }

  return (await response.json()) as WorkflowSummary;
}

export async function fetchWorkflowHistory(): Promise<WorkflowSummary[]> {
  const response = await fetch(`${API_BASE_URL}/api/workflows`);

  if (!response.ok) {
    throw new Error(`workflow history fetch failed with status ${response.status}`);
  }

  const data = (await response.json()) as WorkflowHistoryResponse;
  return data.workflows;
}

export function openWorkflowEventStream(
  workflowId: string,
  handlers: {
    onEvent: (event: Event) => void;
    onError?: () => void;
  },
): EventSource {
  const source = new EventSource(`${API_BASE_URL}/api/workflows/${workflowId}/stream`);
  source.addEventListener("workflow-event", (message) => {
    const parsed = JSON.parse((message as MessageEvent<string>).data) as Event;
    handlers.onEvent(parsed);
  });
  if (handlers.onError) {
    source.onerror = () => handlers.onError?.();
  }
  return source;
}

export async function submitApproval(workflowId: string, request: ApprovalRequest): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/api/workflows/${workflowId}/approval`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    throw new Error(`approval request failed with status ${response.status}`);
  }
}
