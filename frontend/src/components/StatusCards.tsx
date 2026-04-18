import type { Event } from "../../../shared/event-schema/event";

interface AgentCard {
  id: string;
  title: string;
  topicPrefix: string;
}

const cards: AgentCard[] = [
  { id: "gateway", title: "API Gateway", topicPrefix: "mesh.request" },
  { id: "planner", title: "Planner", topicPrefix: "mesh.plan" },
  { id: "retriever", title: "Retriever", topicPrefix: "mesh.result.retrieval" },
  { id: "classifier", title: "Classifier", topicPrefix: "mesh.result.classification" },
  { id: "approval", title: "Approval", topicPrefix: "mesh.approval." },
  { id: "responder", title: "Responder", topicPrefix: "mesh.result.response" },
  { id: "aggregator", title: "Aggregator", topicPrefix: "mesh.workflow." },
];

function statusForCard(topicPrefix: string, events: Event[]): "idle" | "active" | "done" | "failed" {
  const matching = events.filter((event) =>
    topicPrefix.endsWith(".") ? event.topic.startsWith(topicPrefix) : event.topic === topicPrefix,
  );
  if (matching.length === 0) {
    return "idle";
  }
  const latest = matching[matching.length - 1];
  if (latest.status === "failed" || latest.topic === "mesh.workflow.failed") {
    return "failed";
  }
  if (latest.status === "succeeded") {
    return "done";
  }
  return "active";
}

export function StatusCards({ events }: { events: Event[] }) {
  return (
    <section className="status-grid">
      {cards.map((card) => {
        const status = statusForCard(card.topicPrefix, events);
        return (
          <article key={card.id} className={`status-card status-${status}`}>
            <p className="status-label">{card.title}</p>
            <strong>
              {status === "done"
                ? "Complete"
                : status === "failed"
                  ? "Failed"
                  : status === "active"
                    ? "In Progress"
                    : "Waiting"}
            </strong>
            <span>{card.topicPrefix}</span>
          </article>
        );
      })}
    </section>
  );
}
