---
policy_version: "1"
protected: true
---
# Portable AI Memory — Start Here

This vault is a user-owned, Obsidian-readable memory system shared across AI models, providers, and agent harnesses.

## Entry Flow for Agents

1. Call `open_context` first to receive your `context_id`, permitted scopes, `policy_version`, and a scope-filtered bootstrap packet.
2. Call `start_session` with an accurate `capture_coverage` label (`full_accessible_transcript`, `partial_events`, or `summary_only`).
3. Use `search_memory` and `read_page` to retrieve only what is relevant within your permitted scopes.
4. Capture original events with `append_events`, and propose or apply managed updates via `create_memory`, `update_memory`, `create_task`, `update_task`, `ingest_document`, `archive_record`, or `forget_record`.
5. Call `end_session` when wrapping up so consolidation can run cleanly.

## Operating Rules

- [[System/Rules/READ]] — Scoped retrieval and provenance rules
- [[System/Rules/CREATE]] — Record creation and evidence requirements
- [[System/Rules/UPDATE]] — Revision checks, corrections vs. real-world changes
- [[System/Rules/DELETE]] — Archiving vs. explicit forgetting
- [[System/Rules/MEMORY]] — Epistemic discipline for facts, preferences, decisions, states, lessons, and summaries
- [[System/Rules/TASKS]] — Task lifecycle, proposals, scheduling, and daily views

## Areas

- [[Shared/INDEX|Shared]] — Explicitly shared preferences and knowledge
- [[Work/INDEX|Work]] — Internships, corporate activity, work projects, sessions, memories, documents
- [[Personal/INDEX|Personal]] — Life, skills, personal projects, open-source, sessions, memories, documents

## Key Generated Views

- [[PROFILE]] — Stable owner introduction (preserved manual edits)
- [[SUMMARY]] — Derived current overview for the owner
- [[TODAY]] — Daily actionable task view
