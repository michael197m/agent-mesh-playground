import { useEffect, useMemo, useState } from "react";
import type { Event } from "../../../shared/event-schema/event";
import { fetchWorkflowEvents } from "../lib/api";

interface WorkflowEventsState {
  events: Event[];
  isLoading: boolean;
  error: string | null;
}

function sortEvents(events: Event[]): Event[] {
  return [...events].sort((left, right) => {
    const timeDelta = new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime();
    if (timeDelta !== 0) {
      return timeDelta;
    }
    return left.id.localeCompare(right.id);
  });
}

function areEventsEqual(left: Event[], right: Event[]): boolean {
  if (left.length !== right.length) {
    return false;
  }

  return left.every((event, index) => {
    const other = right[index];
    return JSON.stringify(event) === JSON.stringify(other);
  });
}

export function useWorkflowEvents(workflowId: string | null, pollIntervalMs = 2000): WorkflowEventsState {
  const [events, setEvents] = useState<Event[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!workflowId) {
      setEvents([]);
      setIsLoading(false);
      setError(null);
      return;
    }

    let cancelled = false;

    async function loadEvents(activeWorkflowId: string) {
      setIsLoading((current) => current || events.length === 0);
      try {
        const nextEvents = sortEvents(await fetchWorkflowEvents(activeWorkflowId));
        if (cancelled) {
          return;
        }
        setEvents((current) => (areEventsEqual(current, nextEvents) ? current : nextEvents));
        setError(null);
      } catch (loadError) {
        if (cancelled) {
          return;
        }
        const message = loadError instanceof Error ? loadError.message : "failed to load workflow events";
        setError(message);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    void loadEvents(workflowId);
    const intervalId = window.setInterval(() => {
      void loadEvents(workflowId);
    }, pollIntervalMs);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [workflowId, pollIntervalMs, events.length]);

  return useMemo(
    () => ({ events, isLoading, error }),
    [events, isLoading, error],
  );
}
