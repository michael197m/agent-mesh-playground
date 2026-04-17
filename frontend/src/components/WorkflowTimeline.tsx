import type { Event } from "../../../shared/event-schema/event";

function eventTitle(event: Event): string {
  const eventName = typeof event.payload?.event_name === "string" ? event.payload.event_name : null;
  return eventName ?? event.topic;
}

function eventSummary(event: Event): string {
  const payload = event.payload ?? {};

  if (typeof payload.response === "string") {
    return payload.response;
  }
  if (typeof payload.summary === "string") {
    return payload.summary;
  }
  if (typeof payload.classification_result === "string") {
    return payload.classification_result;
  }
  if (typeof payload.reason === "string") {
    return payload.reason;
  }
  return "No additional payload summary available.";
}

export function WorkflowTimeline({ events }: { events: Event[] }) {
  return (
    <section className="panel timeline-panel">
      <div className="panel-header">
        <div>
          <p className="eyebrow">Workflow Activity</p>
          <h2>Timeline</h2>
        </div>
        <span className="timeline-count">{events.length} event(s)</span>
      </div>
      <div className="timeline-list">
        {events.length === 0 ? (
          <div className="empty-state">No events received yet.</div>
        ) : (
          events.map((event) => (
            <article className="timeline-item" key={event.id}>
              <div className="timeline-dot" />
              <div className="timeline-body">
                <div className="timeline-row">
                  <strong>{eventTitle(event)}</strong>
                  <span>{new Date(event.timestamp).toLocaleTimeString()}</span>
                </div>
                <div className="timeline-row timeline-meta">
                  <span>{event.source}</span>
                  <span>{event.topic}</span>
                  <span className={`pill pill-${event.status}`}>{event.status}</span>
                </div>
                <p>{eventSummary(event)}</p>
              </div>
            </article>
          ))
        )}
      </div>
    </section>
  );
}
