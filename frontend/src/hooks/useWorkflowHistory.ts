import { useEffect, useMemo, useState } from "react";
import type { WorkflowSummary } from "../lib/api";
import { fetchWorkflowHistory } from "../lib/api";

interface WorkflowHistoryState {
  workflows: WorkflowSummary[];
  isLoading: boolean;
  error: string | null;
}

export function useWorkflowHistory(refreshToken = 0): WorkflowHistoryState {
  const [workflows, setWorkflows] = useState<WorkflowSummary[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadHistory() {
      setIsLoading(true);
      try {
        const nextWorkflows = await fetchWorkflowHistory();
        if (cancelled) {
          return;
        }
        setWorkflows(nextWorkflows);
        setError(null);
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        const message = loadError instanceof Error ? loadError.message : "failed to load workflow history";
        setError(message);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void loadHistory();

    return () => {
      cancelled = true;
    };
  }, [refreshToken]);

  return useMemo(
    () => ({ workflows, isLoading, error }),
    [workflows, isLoading, error],
  );
}
