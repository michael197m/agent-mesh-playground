import { render, screen } from "@testing-library/react";
import { WorkflowResult } from "./WorkflowResult";
import type { Event } from "../../../shared/event-schema/event";

function buildEvent(overrides: Partial<Event> & Pick<Event, "id" | "topic">): Event {
  return {
    id: overrides.id,
    topic: overrides.topic,
    status: overrides.status ?? "succeeded",
    source: overrides.source ?? "test",
    timestamp: overrides.timestamp ?? "2026-04-17T22:00:00Z",
    payload: overrides.payload ?? {},
  };
}

describe("WorkflowResult", () => {
  it("renders the completed workflow response and recommended actions", () => {
    const events: Event[] = [
      buildEvent({
        id: "retrieval",
        topic: "mesh.result.retrieval",
        payload: {
          summary: "Runbook suggests checking MTU and rollback options.",
        },
      }),
      buildEvent({
        id: "classification",
        topic: "mesh.result.classification",
        payload: {
          classification_result: "severity=high; category=network-config; confidence=0.93",
        },
      }),
      buildEvent({
        id: "response",
        topic: "mesh.result.response",
        payload: {
          response: "Packet loss likely relates to the recent config change.",
          recommended_actions: [
            "Compare the active config with the last known-good version.",
            "Check MTU and interface error counters.",
          ],
        },
      }),
      buildEvent({
        id: "completed",
        topic: "mesh.workflow.completed",
        payload: {
          intent: "incident triage",
        },
      }),
    ];

    render(<WorkflowResult events={events} summary={null} />);

    expect(screen.getByText("Latest Result")).toBeInTheDocument();
    expect(screen.getByText("Packet loss likely relates to the recent config change.")).toBeInTheDocument();
    expect(screen.getByText("Compare the active config with the last known-good version.")).toBeInTheDocument();
    expect(screen.getByText("severity=high; category=network-config; confidence=0.93")).toBeInTheDocument();
    expect(screen.getByText("Runbook suggests checking MTU and rollback options.")).toBeInTheDocument();
    expect(screen.getByText("incident triage")).toBeInTheDocument();
  });

  it("renders the failure reason when the workflow fails", () => {
    const events: Event[] = [
      buildEvent({
        id: "failed",
        topic: "mesh.workflow.failed",
        status: "failed",
        payload: {
          reason: "workflow timed out waiting for the responder",
        },
      }),
    ];

    render(<WorkflowResult events={events} summary={null} />);

    expect(screen.getByText("failed")).toBeInTheDocument();
    expect(screen.getByText("workflow timed out waiting for the responder")).toBeInTheDocument();
    expect(screen.getByText("No response was available before the workflow failed.")).toBeInTheDocument();
  });
});
