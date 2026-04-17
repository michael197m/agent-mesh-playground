import { FormEvent, useState } from "react";

interface RequestFormProps {
  isSubmitting: boolean;
  onSubmit: (prompt: string) => Promise<void>;
}

export function RequestForm({ isSubmitting, onSubmit }: RequestFormProps) {
  const [prompt, setPrompt] = useState("");

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
