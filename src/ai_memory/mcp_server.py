"""MCP JSON-RPC 2.0 adapter (supports Streamable HTTP and stdio) wrapping MemoryCore."""

from __future__ import annotations

import json
import sys
from typing import Any

from .access import ClientIdentity
from .core import MemoryCore

MCP_PROTOCOL_VERSION = "2025-03-26"

SERVER_INSTRUCTIONS = (
    "Portable AI Memory Service. Follow this mandatory lifecycle on every session:\n"
    "1. Call `open_context` FIRST to get `context_id`, permitted scopes, and inline operating rules.\n"
    "2. Whenever the user shares facts, temporary activities/experiments, preferences, decisions, or task/reminder requests, "
    "start a session with `start_session` and call `append_events` with the FULL accessible user message verbatim "
    "(never drop a clause; use field `speaker: 'user'`, NOT `role`).\n"
    "3. Persist EVERY distinct item citing the captured `evt_*` ID in `sources: [{\"record_id\": \"evt_...\", \"quote\": \"...\"}]`:\n"
    "   - Temporary activity/experiment (e.g. 'trying Go this weekend') -> `create_memory` with `type: 'state'`, `origin: 'explicit_user_statement'`, and `valid_from`/`valid_until` (YYYY-MM-DD) when a window is stated (never a lasting `preference`).\n"
    "   - Actionable task/reminder -> `create_task` with `origin: 'explicit_user_statement'` (or `origin: 'assistant_proposal'`, `proposal: true`, `status: 'inbox'` for assistant suggestions).\n"
    "   - Assistant suggestions are NEVER `origin: 'explicit_user_statement'` or accepted `current` decisions.\n"
    "4. Always pass a unique `idempotency_key` on every mutating call.\n"
    "5. Always call `end_session(session_id=...)` before finishing a write/capture turn so consolidation and handoff run."
)

_SOURCE_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["record_id"],
    "additionalProperties": False,
    "properties": {
        "record_id": {
            "type": "string",
            "description": "Existing captured event ID (evt_...) or record ID. Capture user text via start_session + append_events first.",
        },
        "quote": {
            "type": "string",
            "description": "Optional verbatim substring from the referenced source content.",
        },
        "page": {"type": "integer", "minimum": 1},
        "revision": {"type": "integer", "minimum": 1},
    },
}

_RELATION_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["type", "target_id"],
    "additionalProperties": False,
    "properties": {
        "type": {
            "type": "string",
            "enum": ["supports", "contradicts", "supersedes", "summarizes", "part_of", "related_to"],
        },
        "target_id": {"type": "string"},
    },
}

_EVENT_ITEM_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["speaker", "content"],
    "additionalProperties": False,
    "properties": {
        "speaker": {
            "type": "string",
            "enum": ["user", "assistant", "system", "tool", "external"],
            "description": "Message speaker (use 'speaker', not 'role').",
        },
        "content": {
            "type": "string",
            "description": "Full accessible message text verbatim; do not drop clauses.",
        },
        "event_type": {
            "type": "string",
            "enum": ["message", "note", "correction", "tool_result", "import", "decision_note"],
            "default": "message",
        },
        "source_event_id": {
            "type": "string",
            "description": "Optional stable upstream ID for deduplication.",
        },
        "sequence": {"type": "integer", "minimum": 0},
        "occurred_at": {"type": "string", "description": "Optional ISO-8601 UTC timestamp if known."},
        "attachment_ids": {"type": "array", "items": {"type": "string"}},
        "source_reference": {"type": "string"},
    },
}

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "open_context",
        "description": "Open a scoped application context and receive a compact, scope-filtered bootstrap packet with inline mandatory operating rules. Always call this first.",
        "inputSchema": {
            "type": "object",
            "required": ["purpose"],
            "properties": {
                "purpose": {"type": "string", "description": "Brief description of the current session purpose."},
                "scopes": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional subset of scopes to request (intersected with credential permissions).",
                },
                "project_id": {"type": "string"},
            },
        },
    },
    {
        "name": "start_session",
        "description": "Start a tracked capture session with an explicit capture_coverage disclosure. Always close it with end_session when done.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "source_application", "capture_coverage"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "source_application": {"type": "string", "description": "Harness or application name (e.g. 'codex', 'antigravity')."},
                "capture_coverage": {
                    "type": "string",
                    "enum": ["full_accessible_transcript", "partial_events", "summary_only"],
                    "description": "Use 'partial_events' unless BOTH user and assistant turns are appended via append_events.",
                },
                "scope_id": {"type": "string", "description": "Owning scope (e.g. 'personal', 'work', 'shared')."},
                "project_id": {"type": "string"},
                "title": {"type": "string"},
                "folder": {"type": "string"},
                "known_gaps": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "append_events",
        "description": "Append immutable original events to a session. Capture the full accessible user message verbatim using field 'speaker' (not 'role'). Returns evt_* IDs to cite in sources.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "session_id", "events"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "session_id": {"type": "string"},
                "events": {
                    "type": "array",
                    "minItems": 1,
                    "items": _EVENT_ITEM_SCHEMA,
                },
            },
        },
    },
    {
        "name": "search_memory",
        "description": "Search permitted memories, tasks, projects, sessions, and documents within the active context's scopes.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "query"],
            "properties": {
                "context_id": {"type": "string"},
                "query": {"type": "string"},
                "kind": {"type": "string", "enum": ["memory", "task", "project", "session", "document"]},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "include_archived": {"type": "boolean"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
        },
    },
    {
        "name": "read_page",
        "description": "Read an authorized record by ID (mem_*, task_*, prj_*, ses_*, doc_*, evt_*) or an allowlisted policy path (e.g. 'START-HERE.md', 'System/Rules/CREATE.md').",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "record_id"],
            "properties": {
                "context_id": {"type": "string"},
                "record_id": {"type": "string"},
                "max_chars": {"type": "integer", "minimum": 1},
            },
        },
    },
    {
        "name": "create_memory",
        "description": "Create a sourced maintained memory. Use type='state' for temporary activities/experiments (never 'preference'). Cite real evt_* IDs in sources.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "type", "scope_id", "title", "body", "origin", "sources"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "type": {"type": "string", "enum": ["fact", "preference", "decision", "state", "lesson", "summary"]},
                "scope_id": {"type": "string"},
                "project_id": {"type": "string"},
                "title": {"type": "string"},
                "body": {"type": "string"},
                "folder": {"type": "string"},
                "origin": {
                    "type": "string",
                    "enum": ["explicit_user_statement", "observation", "inference", "import"],
                    "description": "Use 'explicit_user_statement' when citing a user-spoken event.",
                },
                "status": {
                    "type": "string",
                    "enum": ["current", "uncertain", "disputed", "superseded", "retracted"],
                },
                "occurred_at": {"type": "string"},
                "valid_from": {"type": "string"},
                "valid_until": {"type": "string"},
                "sources": {"type": "array", "minItems": 1, "items": _SOURCE_ITEM_SCHEMA},
                "relations": {"type": "array", "items": _RELATION_ITEM_SCHEMA},
            },
        },
    },
    {
        "name": "update_memory",
        "description": "Update an existing memory with optimistic revision checking and external-edit protection.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "record_id", "expected_revision", "changes", "reason"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "record_id": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 1},
                "changes": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                        "status": {"type": "string", "enum": ["current", "uncertain", "disputed", "superseded", "retracted"]},
                        "valid_from": {"type": ["string", "null"]},
                        "valid_until": {"type": ["string", "null"]},
                        "occurred_at": {"type": ["string", "null"]},
                        "sources": {"type": "array", "minItems": 1, "items": _SOURCE_ITEM_SCHEMA},
                        "relations": {"type": "array", "items": _RELATION_ITEM_SCHEMA},
                    },
                },
                "reason": {"type": "string"},
                "source_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "create_task",
        "description": "Create an authoritative task record citing captured evt_* IDs in sources. Assistant suggestions must use origin='assistant_proposal' and enter 'inbox'.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "scope_id", "title", "sources"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "scope_id": {"type": "string"},
                "project_id": {"type": "string"},
                "title": {"type": "string"},
                "body": {"type": "string"},
                "folder": {"type": "string"},
                "origin": {
                    "type": "string",
                    "enum": ["explicit_user_statement", "observation", "inference", "import", "assistant_proposal"],
                    "default": "explicit_user_statement",
                },
                "proposal": {"type": "boolean"},
                "status": {"type": "string", "enum": ["inbox", "next", "in_progress", "blocked", "done", "cancelled"]},
                "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"], "default": "normal"},
                "scheduled_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$", "description": "YYYY-MM-DD"},
                "due_date": {"type": "string", "pattern": "^\\d{4}-\\d{2}-\\d{2}$", "description": "YYYY-MM-DD"},
                "blocked_reason": {"type": "string"},
                "depends_on": {"type": "array", "items": {"type": "string"}},
                "sources": {"type": "array", "minItems": 1, "items": _SOURCE_ITEM_SCHEMA},
                "relations": {"type": "array", "items": _RELATION_ITEM_SCHEMA},
            },
        },
    },
    {
        "name": "update_task",
        "description": "Update a task record with state-machine validation, cycle checks, and revision locking.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "record_id", "expected_revision", "changes"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "record_id": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 1},
                "changes": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "body": {"type": "string"},
                        "status": {"type": "string", "enum": ["inbox", "next", "in_progress", "blocked", "done", "cancelled"]},
                        "priority": {"type": "string", "enum": ["low", "normal", "high", "urgent"]},
                        "scheduled_date": {"type": ["string", "null"]},
                        "due_date": {"type": ["string", "null"]},
                        "blocked_reason": {"type": ["string", "null"]},
                        "depends_on": {"type": "array", "items": {"type": "string"}},
                        "sources": {"type": "array", "minItems": 1, "items": _SOURCE_ITEM_SCHEMA},
                        "relations": {"type": "array", "items": _RELATION_ITEM_SCHEMA},
                    },
                },
                "reason": {"type": "string"},
                "source_ids": {"type": "array", "items": {"type": "string"}},
                "explicit_user_instruction": {"type": "boolean"},
            },
        },
    },
    {
        "name": "ingest_document",
        "description": "Store an original attachment and create a companion document record with queued extraction.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "scope_id", "original_filename", "media_type", "content_base64"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "scope_id": {"type": "string"},
                "project_id": {"type": "string"},
                "folder": {"type": "string"},
                "original_filename": {"type": "string"},
                "media_type": {"type": "string"},
                "content_base64": {"type": "string"},
                "title": {"type": "string"},
            },
        },
    },
    {
        "name": "end_session",
        "description": "Close an open capture session, optionally append final events, and enqueue background consolidation. Always call this before finishing a write/capture turn.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "session_id"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "session_id": {"type": "string"},
                "final_events": {"type": "array", "items": _EVENT_ITEM_SCHEMA},
            },
        },
    },
    {
        "name": "archive_record",
        "description": "Reversibly archive a record so it is hidden from routine active views.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "record_id", "expected_revision", "reason"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "record_id": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 1},
                "reason": {"type": "string"},
            },
        },
    },
    {
        "name": "forget_record",
        "description": "Explicitly delete a record or event, clear derived copies and revision history, and record a content-free tombstone.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "record_id", "reason", "confirm_forget"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "record_id": {"type": "string"},
                "expected_revision": {"type": "integer", "minimum": 1},
                "reason": {"type": "string"},
                "confirm_forget": {"type": "boolean"},
            },
        },
    },
    {
        "name": "list_areas",
        "description": "List dynamic top-level areas and their subfolders permitted in the active context.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id"],
            "properties": {
                "context_id": {"type": "string"},
            },
        },
    },
    {
        "name": "register_area",
        "description": "Dynamically register a new top-level area or add subfolders to an existing area.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "slug"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "slug": {"type": "string"},
                "folder": {"type": "string"},
                "description": {"type": "string"},
                "subfolders": {"type": "array", "items": {"type": "string"}},
                "has_tasks_dashboard": {"type": "boolean"},
            },
        },
    },
]


def handle_jsonrpc(core: MemoryCore, client: ClientIdentity, request: dict[str, Any]) -> dict[str, Any] | None:
    """Process one MCP JSON-RPC 2.0 request and return a response dict (or None for notifications)."""
    req_id = request.get("id")
    method = request.get("method")
    params = request.get("params") or {}

    if method == "notifications/initialized":
        return None

    if method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {"tools": {"listChanged": False}},
                "serverInfo": {"name": "ai-memory", "version": "0.1.0"},
                "instructions": SERVER_INSTRUCTIONS,
            },
        }

    if method == "tools/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOL_DEFINITIONS}}

    if method == "tools/call":
        tool_name = params.get("name")
        arguments = dict(params.get("arguments") or {})
        context_id = arguments.pop("context_id", None)
        idempotency_key = arguments.pop("idempotency_key", None)
        envelope = core.dispatch(
            client=client,
            operation=str(tool_name or ""),
            payload=arguments,
            context_id=context_id,
            idempotency_key=idempotency_key,
        )
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(envelope, ensure_ascii=False)}],
                "structuredContent": envelope,
                "isError": not envelope.get("ok", False),
            },
        }

    return {
        "jsonrpc": "2.0",
        "id": req_id,
        "error": {"code": -32601, "message": f"Method not found: {method}"},
    }


def serve_stdio(core: MemoryCore, client: ClientIdentity) -> None:
    """Run MCP JSON-RPC 2.0 over newline-delimited stdin/stdout."""
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError as exc:
            err = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": str(exc)}}
            sys.stdout.write(json.dumps(err) + "\n")
            sys.stdout.flush()
            continue
        resp = handle_jsonrpc(core, client, req)
        if resp is not None:
            sys.stdout.write(json.dumps(resp, ensure_ascii=False) + "\n")
            sys.stdout.flush()
