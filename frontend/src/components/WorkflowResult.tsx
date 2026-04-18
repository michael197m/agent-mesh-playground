import type { Event } from "../../../shared/event-schema/event";
import type { WorkflowSummary } from "../lib/api";

interface WorkflowResultProps {
  events: Event[];
  summary: WorkflowSummary | null;
}

function lastEventForTopic(events: Event[], topic: string): Event | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (events[index].topic === topic) {
      return events[index];
    }
  }
  return null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() !== "" ? value : null;
}

function asStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.filter((item): item is string => typeof item === "string" && item.trim() !== "");
}

export function WorkflowResult({ events, summary }: WorkflowResultProps) {
  const completionEvent = lastEventForTopic(events, "mesh.workflow.completed");
  const failureEvent = lastEventForTopic(events, "mesh.workflow.failed");
  const responseEvent = lastEventForTopic(events, "mesh.result.response");
  const classificationEvent = lastEventForTopic(events, "mesh.result.classification");
  const retrievalEvent = lastEventForTopic(events, "mesh.result.retrieval");

  const terminalEvent = failureEvent ?? completionEvent;
  const terminalPayload = terminalEvent?.payload ?? {};
  const responsePayload =
    responseEvent?.payload ??
    summary?.response_result ??
    (terminalPayload.response_result as Record<string, unknown> | undefined) ??
    {};
  const classificationPayload =
    classificationEvent?.payload ??
    summary?.classification_result ??
    (terminalPayload.classification_result as Record<string, unknown> | undefined) ??
    {};
  const retrievalPayload =
    retrievalEvent?.payload ??
    summary?.retrieval_result ??
    (terminalPayload.retrieval_result as Record<string, unknown> | undefined) ??
    {};

  const response = asString(responsePayload.response);
  const recommendedActions = asStringArray(responsePayload.recommended_actions);
  const classification = asString(classificationPayload.classification_result);
  const retrievalSummary = asString(retrievalPayload.summary);
  const intent = asString(summary?.intent) ?? asString(terminalPayload.intent);
  const failureReason = asString(summary?.failure_reason) ?? asString(terminalPayload.reason);
  const terminalStatus = summary?.status ?? (failureEvent ? "failed" : completionEvent ? "completed" : "");
  const isFailed = terminalStatus === "failed";

  return (
    <section className="panel result-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Workflow Output</p>
          <h2>Latest Result</h2>
        </div>
        {terminalStatus ? <span className={`pill ${isFailed ? "pill-failed" : "pill-succeeded"}`}>{terminalStatus}</span> : null}
      </div>

      {!terminalStatus ? (
        <div className="empty-state">Run a workflow to populate the final response panel.</div>
      ) : (
        <div className="result-grid">
          {isFailed ? (
            <div className="result-block result-block-failed">
              <p className="result-label">Failure</p>
              <p className="result-text">{failureReason ?? "The workflow ended in a failed state."}</p>
            </div>
          ) : null}

          <div className="result-block">
            <p className="result-label">Response</p>
            <p className="result-text">
              {response ?? (isFailed ? "No response was available before the workflow failed." : "No final response was emitted.")}
            </p>
          </div>

          {recommendedActions.length > 0 ? (
            <div className="result-block">
              <p className="result-label">Recommended Actions</p>
              <ul className="result-list">
                {recommendedActions.map((action) => (
                  <li key={action}>{action}</li>
                ))}
              </ul>
            </div>
          ) : null}

          {classification ? (
            <div className="result-block">
              <p className="result-label">Classification</p>
              <p className="result-text">{classification}</p>
            </div>
          ) : null}

          {retrievalSummary ? (
            <div className="result-block">
              <p className="result-label">Retrieved Context</p>
              <p className="result-text">{retrievalSummary}</p>
            </div>
          ) : null}

          {intent ? (
            <div className="result-block">
              <p className="result-label">Intent</p>
              <p className="result-text">{intent}</p>
            </div>
          ) : null}
        </div>
      )}
    </section>
  );
}
