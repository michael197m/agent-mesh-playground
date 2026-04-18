import { FormEvent, useState } from "react";

interface RequestFormProps {
  isSubmitting: boolean;
  onSubmit: (prompt: string) => Promise<void>;
}

export function RequestForm({ isSubmitting, onSubmit }: RequestFormProps) {
  const [prompt, setPrompt] = useState("");
  const scenarios = [
    "Our Toronto edge site is showing intermittent packet loss after a config update. Classify severity, retrieve relevant context, and recommend next steps.",
    "Review packet loss and prepare a rollback recommendation for the production edge firewall if the latest config is the cause.",
    "Investigate elevated VPN tunnel latency after a route policy change and summarize the likely operator actions.",
  ];

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const trimmedPrompt = prompt.trim();
    if (!trimmedPrompt) {
      return;
    }

    await onSubmit(trimmedPrompt);
  }

  return (
    <form className="panel request-form" onSubmit={handleSubmit}>
      <div className="panel-header">
        <div>
          <p className="eyebrow">New Workflow</p>
          <h2>Request Intake</h2>
        </div>
      </div>
      <label className="field-label" htmlFor="prompt">
        Describe the operator request
      </label>
      <div className="scenario-list">
        {scenarios.map((scenario) => (
          <button
            key={scenario}
            className="scenario-chip"
            type="button"
            disabled={isSubmitting}
            onClick={() => setPrompt(scenario)}
          >
            {scenario}
          </button>
        ))}
      </div>
      <textarea
        id="prompt"
        className="request-input"
        placeholder="Example: Investigate intermittent packet loss on the VPN edge and draft an operator response."
        rows={6}
        value={prompt}
        onChange={(event) => setPrompt(event.target.value)}
        disabled={isSubmitting}
      />
      <div className="request-actions">
        <button className="primary-button" type="submit" disabled={isSubmitting || prompt.trim().length === 0}>
          {isSubmitting ? "Submitting..." : "Start Workflow"}
        </button>
      </div>
    </form>
  );
}
