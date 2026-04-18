import type { WorkflowSummary } from "../lib/api";

interface WorkflowHistoryProps {
  workflows: WorkflowSummary[];
  activeWorkflowId: string | null;
  isLoading: boolean;
  error: string | null;
  onSelect: (workflow: WorkflowSummary) => void;
}

function statusClass(status: string): string {
  switch (status) {
    case "completed":
      return "pill-succeeded";
    case "failed":
      return "pill-failed";
    default:
      return "pill-running";
  }
}

export function WorkflowHistory({ workflows, activeWorkflowId, isLoading, error, onSelect }: WorkflowHistoryProps) {
  return (
    <section className="panel history-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Workflow History</p>
          <h2>Recent Runs</h2>
        </div>
      </div>

      {error ? <p className="error-text">{error}</p> : null}
      {isLoading ? <p className="hint-text">Loading recent workflows...</p> : null}

      {workflows.length === 0 ? (
        <div className="empty-state">No workflows have been recorded yet.</div>
      ) : (
        <div className="history-list">
          {workflows.map((workflow) => (
            <button
              key={workflow.workflow_id}
              className={`history-item ${workflow.workflow_id === activeWorkflowId ? "history-item-active" : ""}`}
              type="button"
              onClick={() => onSelect(workflow)}
            >
              <div className="history-row">
                <strong>{workflow.intent || "workflow run"}</strong>
                <span className={`pill ${statusClass(workflow.status)}`}>{workflow.status}</span>
              </div>
              <p className="history-prompt">{workflow.prompt}</p>
              <div className="history-row history-meta">
                <span>{workflow.event_count} event(s)</span>
                <span>{new Date(workflow.updated_at).toLocaleString()}</span>
              </div>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}
