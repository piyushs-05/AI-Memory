---
policy_version: "1"
protected: true
---
# Rule: MEMORY

- **Types:** `fact`, `preference`, `decision`, `state`, `lesson`, `summary`.
- **Statuses:** `current`, `uncertain`, `disputed`, `superseded`, `retracted`.
- **Temporary activity** (e.g., evaluating a tool for a week) is stored as a `state`, never promoted to a lasting `preference`.
- **Inferences** must be labelled `origin: inference` and `status: uncertain`; confidence alone cannot promote an inference to an explicit fact.
- **Unexplained contradictions** retain both claims and mark them `disputed` with a `contradicts` relation.
- **Decisions** capture rationale, alternatives when known, and acceptance evidence.
- **Summaries** list covered source IDs/revisions; repetition through summaries does not count as independent evidence.
