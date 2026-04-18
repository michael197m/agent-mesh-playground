import { useEffect, useMemo, useState } from "react";
import type { Event } from "../../../shared/event-schema/event";
import { fetchWorkflowEvents, openWorkflowEventStream } from "../lib/api";

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

function mergeEvents(current: Event[], incoming: Event[]): Event[] {
  const byID = new Map(current.map((event) => [event.id, event]));
  for (const event of incoming) {
    byID.set(event.id, event);
  }
  return sortEvents([...byID.values()]);
}

export function useWorkflowEvents(workflowId: string | null): WorkflowEventsState {
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
    let stream: EventSource | null = null;

    async function loadEvents(activeWorkflowId: string) {
      setIsLoading(true);
      try {
        const nextEvents = sortEvents(await fetchWorkflowEvents(activeWorkflowId));
        if (cancelled) {
          return;
        }
        setEvents(nextEvents);
        setError(null);

        stream = openWorkflowEventStream(activeWorkflowId, {
          onEvent: (event) => {
            if (cancelled) {
              return;
            }
            setEvents((current) => mergeEvents(current, [event]));
          },
        });
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

    return () => {
      cancelled = true;
      stream?.close();
    };
  }, [workflowId]);

  return useMemo(
    () => ({ events, isLoading, error }),
    [events, isLoading, error],
  );
}
