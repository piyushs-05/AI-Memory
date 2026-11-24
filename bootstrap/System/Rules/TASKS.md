---
policy_version: "1"
protected: true
---
# Rule: TASKS

- Each task is a separate authoritative Markdown record. Area `TASKS.md` and root `TODAY.md` are derived projections.
- **State machine:**
  - `inbox -> next -> in_progress -> done`
  - `next / in_progress -> blocked -> next / in_progress`
  - Any unfinished state -> `cancelled`
  - `done / cancelled -> next` only through explicit reopen with a `reason` (preserving `completion_history`).
- `blocked` requires a `blocked_reason` or an unresolved `depends_on` task.
- `done` requires supporting evidence or explicit user instruction and sets `completed_at`.
- Prevent dependency cycles. Never silently reschedule overdue tasks or invent deadlines.
