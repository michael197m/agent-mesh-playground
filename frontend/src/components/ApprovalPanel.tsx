import { useMemo, useState } from "react";
import type { Event } from "../../../shared/event-schema/event";
import { submitApproval } from "../lib/api";

interface ApprovalPanelProps {
  workflowId: string | null;
  events: Event[];
}

function lastEventForTopic(events: Event[], topic: string): Event | null {
  for (let index = events.length - 1; index >= 0; index -= 1) {
    if (events[index].topic === topic) {
      return events[index];
    }
  }
  return null;
}

function isPendingApproval(events: Event[]): boolean {
  const required = lastEventForTopic(events, "mesh.approval.required");
  if (!required) {
    return false;
  }
  if (lastEventForTopic(events, "mesh.workflow.completed") || lastEventForTopic(events, "mesh.workflow.failed")) {
    return false;
  }
  const received = lastEventForTopic(events, "mesh.approval.received");
  if (!received) {
    return true;
  }
  const requiredTime = new Date(required.timestamp).getTime();
  const receivedTime = new Date(received.timestamp).getTime();
  if (receivedTime > requiredTime) {
    return false;
  }
  if (receivedTime < requiredTime) {
    return true;
  }
  return received.id < required.id;
}

export function ApprovalPanel({ workflowId, events }: ApprovalPanelProps) {
  const [decisionError, setDecisionError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [comment, setComment] = useState("");

  const approvalRequiredEvent = useMemo(() => lastEventForTopic(events, "mesh.approval.required"), [events]);
  const approvalReceivedEvent = useMemo(() => lastEventForTopic(events, "mesh.approval.received"), [events]);
  const pending = useMemo(() => isPendingApproval(events), [events]);

  async function handleDecision(decision: "approved" | "rejected") {
    if (!workflowId) {
      return;
    }
    setIsSubmitting(true);
    setDecisionError(null);
    try {
      await submitApproval(workflowId, {
        decision,
        comment: comment.trim(),
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : "failed to submit approval";
      setDecisionError(message);
    } finally {
      setIsSubmitting(false);
    }
  }

  if (!approvalRequiredEvent && !approvalReceivedEvent) {
    return null;
  }

  const requiredReason = typeof approvalRequiredEvent?.payload?.reason === "string" ? approvalRequiredEvent.payload.reason : null;
  const approvalDecision = typeof approvalReceivedEvent?.payload?.decision === "string" ? approvalReceivedEvent.payload.decision : null;
  const approvalComment = typeof approvalReceivedEvent?.payload?.comment === "string" ? approvalReceivedEvent.payload.comment : null;

  return (
    <section className="panel approval-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Operator Gate</p>
          <h2>Approval</h2>
        </div>
        {pending ? <span className="pill pill-running">pending</span> : approvalDecision ? <span className={`pill ${approvalDecision === "approved" ? "pill-succeeded" : "pill-failed"}`}>{approvalDecision}</span> : null}
      </div>

      {requiredReason ? (
        <p className="result-text">
          {requiredReason}
        </p>
      ) : (
        <p className="result-text">
          This workflow requires explicit operator approval before the response step can proceed.
        </p>
      )}

      {pending ? (
        <>
          <label className="field-label" htmlFor="approval-comment">
            Approval comment
          </label>
          <textarea
            id="approval-comment"
            className="approval-input"
            rows={3}
            value={comment}
            onChange={(event) => setComment(event.target.value)}
            disabled={isSubmitting}
            placeholder="Optional context for the approval decision."
          />
          <div className="approval-actions">
            <button className="secondary-button" type="button" disabled={isSubmitting} onClick={() => void handleDecision("rejected")}>
              Reject
            </button>
            <button className="primary-button" type="button" disabled={isSubmitting} onClick={() => void handleDecision("approved")}>
              {isSubmitting ? "Submitting..." : "Approve"}
            </button>
          </div>
        </>
      ) : (
        <div className="result-block">
          <p className="result-label">Decision</p>
          <p className="result-text">{approvalDecision ?? "No approval decision recorded."}</p>
          {approvalComment ? (
            <>
              <p className="result-label">Comment</p>
              <p className="result-text">{approvalComment}</p>
            </>
          ) : null}
        </div>
      )}

      {decisionError ? <p className="error-text">{decisionError}</p> : null}
    </section>
  );
}
