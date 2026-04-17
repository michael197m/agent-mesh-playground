export type EventStatus = "pending" | "running" | "succeeded" | "failed";

export interface Event {
  id: string;
  topic: string;
  status: EventStatus;
  source: string;
  timestamp: string;
  payload?: Record<string, unknown>;
}
