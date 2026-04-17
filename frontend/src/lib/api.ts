import type { Event } from "../../../shared/event-schema/event";

export interface ChatRequest {
  prompt: string;
}

export interface ChatResponse {
  workflow_id: string;
  correlation_id: string;
}

export interface WorkflowEventsResponse {
  events: Event[];
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
