import { useEffect, useMemo, useState } from "react";
import type { WorkflowSummary } from "../lib/api";
import { fetchWorkflowSummary } from "../lib/api";

interface WorkflowSummaryState {
  summary: WorkflowSummary | null;
  isLoading: boolean;
  error: string | null;
}

export function useWorkflowSummary(workflowId: string | null, refreshToken = 0): WorkflowSummaryState {
  const [summary, setSummary] = useState<WorkflowSummary | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!workflowId) {
      setSummary(null);
      setIsLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;

    async function loadSummary(activeWorkflowID: string) {
      setIsLoading(true);
      try {
        const nextSummary = await fetchWorkflowSummary(activeWorkflowID);
        if (cancelled) {
          return;
        }
        setSummary(nextSummary);
        setError(null);
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        const message = loadError instanceof Error ? loadError.message : "failed to load workflow summary";
        setError(message);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void loadSummary(workflowId);

    return () => {
      cancelled = true;
    };
  }, [workflowId, refreshToken]);

  return useMemo(
    () => ({ summary, isLoading, error }),
    [summary, isLoading, error],
  );
}
