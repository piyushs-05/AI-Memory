---
policy_version: "1"
protected: true
---
# Rule: CREATE

1. Every mutation must carry a unique `idempotency_key`.
2. Every maintained memory and task must cite at least one valid source (`sources`).
3. Capture speaker roles faithfully: assistant text must never be recorded as an explicit user statement.
4. Do not invent dates, deadlines, urgency, or personal details.
5. Assistant-suggested tasks enter `inbox` with `proposal: true` and `origin: assistant_proposal`.
