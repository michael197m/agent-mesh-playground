import { useMemo, useState } from "react";
import type { Event } from "../../shared/event-schema/event";
import { RequestForm } from "./components/RequestForm";
import { WorkflowHistory } from "./components/WorkflowHistory";
import { StatusCards } from "./components/StatusCards";
import { ApprovalPanel } from "./components/ApprovalPanel";
import { WorkflowResult } from "./components/WorkflowResult";
import { WorkflowTimeline } from "./components/WorkflowTimeline";
import { useWorkflowEvents } from "./hooks/useWorkflowEvents";
import { useWorkflowHistory } from "./hooks/useWorkflowHistory";
import { useWorkflowSummary } from "./hooks/useWorkflowSummary";
import { submitChat } from "./lib/api";
import type { WorkflowSummary } from "./lib/api";

interface WorkflowSession {
  workflowId: string;
  correlationId: string;
}

function buildOptimisticEvent(session: WorkflowSession): Event {
  return {
    id: `${session.workflowId}-pending`,
    topic: "mesh.request",
    status: "pending",
    source: "api-gateway",
    timestamp: new Date().toISOString(),
    payload: {
      event_name: "request.received",
      workflow_id: session.workflowId,
      correlation_id: session.correlationId,
    },
  };
}

export default function App() {
  const [session, setSession] = useState<WorkflowSession | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const { events, isLoading, error: eventsError } = useWorkflowEvents(session?.workflowId ?? null);
  const { summary, error: summaryError } = useWorkflowSummary(session?.workflowId ?? null, events.length);
  const { workflows, isLoading: historyLoading, error: historyError } = useWorkflowHistory(events.length + (session ? 1 : 0));

  const visibleEvents = useMemo(() => {
    if (events.length > 0 || !session) {
      return events;
    }
    return [buildOptimisticEvent(session)];
  }, [events, session]);

  async function handleSubmit(prompt: string) {
    setIsSubmitting(true);
    setSubmitError(null);

    try {
      const response = await submitChat({ prompt });
      setSession({ workflowId: response.workflow_id, correlationId: response.correlation_id });
    } catch (submitFailure) {
      const message = submitFailure instanceof Error ? submitFailure.message : "failed to start workflow";
      setSubmitError(message);
    } finally {
      setIsSubmitting(false);
    }
  }

  function handleSelectWorkflow(workflow: WorkflowSummary) {
    setSession({
      workflowId: workflow.workflow_id,
      correlationId: workflow.correlation_id,
    });
    setSubmitError(null);
  }

  return (
    <main className="app-shell">
      <section className="hero">
        <p className="eyebrow">Agent Mesh Playground</p>
        <h1>Workflow Console</h1>
        <p className="hero-copy">
          Submit an operator request, then watch planner, retrieval, classification, response, and aggregation events arrive in sequence.
        </p>
      </section>

      <div className="layout-grid">
        <div className="left-column">
          <RequestForm isSubmitting={isSubmitting} onSubmit={handleSubmit} />
          <WorkflowHistory
            workflows={workflows}
            activeWorkflowId={session?.workflowId ?? null}
            isLoading={historyLoading}
            error={historyError}
            onSelect={handleSelectWorkflow}
          />
          <section className="panel session-panel">
            <div className="panel-header">
              <div>
                <p className="eyebrow">Current Session</p>
                <h2>Workflow Identifiers</h2>
              </div>
            </div>
            {session ? (
              <dl className="session-details">
                <div>
                  <dt>Workflow ID</dt>
                  <dd>{session.workflowId}</dd>
                </div>
                <div>
                  <dt>Correlation ID</dt>
                  <dd>{session.correlationId}</dd>
                </div>
              </dl>
            ) : (
              <div className="empty-state">Submit a request to create a workflow.</div>
            )}
            {submitError ? <p className="error-text">{submitError}</p> : null}
            {eventsError ? <p className="error-text">{eventsError}</p> : null}
            {summaryError ? <p className="error-text">{summaryError}</p> : null}
            {isLoading ? <p className="hint-text">Streaming backend workflow events...</p> : null}
            {summary ? (
              <p className="hint-text">
                Status: {summary.status} · {summary.event_count} event(s) · updated {new Date(summary.updated_at).toLocaleTimeString()}
              </p>
            ) : (
              <p className="hint-text">The frontend now streams the gateway event feed and uses the workflow summary endpoint for normalized state.</p>
            )}
          </section>
        </div>

        <div className="right-column">
          <StatusCards events={visibleEvents} />
          <ApprovalPanel workflowId={session?.workflowId ?? null} events={visibleEvents} />
          <WorkflowResult events={visibleEvents} summary={summary} />
          <WorkflowTimeline events={visibleEvents} />
        </div>
      </div>
    </main>
  );
}
