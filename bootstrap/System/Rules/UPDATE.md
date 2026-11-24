---
policy_version: "1"
protected: true
---
# Rule: UPDATE

1. Always pass `expected_revision` and a clear `reason` with supporting `source_ids`.
2. Identity (`id`, `recorded_at`) and original events (`records/`) are immutable.
3. Explicit corrections preserve prior history and update standing with the correction source.
4. Real-world changes use time-qualified history (`valid_from`, `valid_until`, `supersedes`) rather than erasing prior truth.
5. Manual Obsidian edits are respected; if a file changed outside the service, reconcile rather than silently overwriting.
6. Protected policy documents under `System/Rules/` and `START-HERE.md` cannot be modified by ordinary memory operations.
