"""MCP JSON-RPC 2.0 adapter (supports Streamable HTTP and stdio) wrapping MemoryCore."""

from __future__ import annotations

import json
import sys
from typing import Any

from .access import ClientIdentity
from .core import MemoryCore

MCP_PROTOCOL_VERSION = "2025-03-26"

SERVER_INSTRUCTIONS = (
    "Portable AI Memory Service. Always call `open_context` first to obtain a `context_id` "
    "and your scope-filtered bootstrap packet, then pass `context_id` on every subsequent call. "
    "Pass a unique `idempotency_key` on every mutating call and cite valid `sources`."
)

TOOL_DEFINITIONS: list[dict[str, Any]] = [
    {
        "name": "open_context",
        "description": "Open a scoped application context and receive a compact, scope-filtered bootstrap packet. Call this first.",
        "inputSchema": {
            "type": "object",
            "required": ["purpose"],
            "properties": {
                "purpose": {"type": "string"},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "project_id": {"type": "string"},
            },
        },
    },
    {
        "name": "start_session",
        "description": "Start a tracked session with an explicit capture_coverage disclosure.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "source_application", "capture_coverage"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "source_application": {"type": "string"},
                "capture_coverage": {"type": "string", "enum": ["full_accessible_transcript", "partial_events", "summary_only"]},
                "scope_id": {"type": "string"},
                "project_id": {"type": "string"},
                "title": {"type": "string"},
                "known_gaps": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "append_events",
        "description": "Append immutable original events to a session with deduplication by source_event_id.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "session_id", "events"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "session_id": {"type": "string"},
                "events": {"type": "array", "items": {"type": "object"}},
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
                "kind": {"type": "string"},
                "scopes": {"type": "array", "items": {"type": "string"}},
                "include_archived": {"type": "boolean"},
                "limit": {"type": "integer"},
            },
        },
    },
    {
        "name": "read_page",
        "description": "Read an authorized record by ID along with scope-filtered related references.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "record_id"],
            "properties": {
                "context_id": {"type": "string"},
                "record_id": {"type": "string"},
                "max_chars": {"type": "integer"},
            },
        },
    },
    {
        "name": "create_memory",
        "description": "Create a sourced maintained memory (fact, preference, decision, state, lesson, or summary).",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "type", "scope_id", "title", "body", "origin", "sources"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "type": {"type": "string"},
                "scope_id": {"type": "string"},
                "project_id": {"type": "string"},
                "title": {"type": "string"},
                "body": {"type": "string"},
                "folder": {"type": "string"},
                "origin": {"type": "string"},
                "status": {"type": "string"},
                "sources": {"type": "array", "items": {"type": "object"}},
                "relations": {"type": "array", "items": {"type": "object"}},
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
                "expected_revision": {"type": "integer"},
                "changes": {"type": "object"},
                "reason": {"type": "string"},
                "source_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    {
        "name": "create_task",
        "description": "Create an authoritative task record. Assistant suggestions enter inbox as proposals.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "scope_id", "title", "sources"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "scope_id": {"type": "string"},
                "title": {"type": "string"},
                "body": {"type": "string"},
                "folder": {"type": "string"},
                "origin": {"type": "string"},
                "proposal": {"type": "boolean"},
                "status": {"type": "string"},
                "priority": {"type": "string"},
                "scheduled_date": {"type": "string"},
                "due_date": {"type": "string"},
                "depends_on": {"type": "array", "items": {"type": "string"}},
                "sources": {"type": "array", "items": {"type": "object"}},
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
                "expected_revision": {"type": "integer"},
                "changes": {"type": "object"},
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
                "original_filename": {"type": "string"},
                "media_type": {"type": "string"},
                "content_base64": {"type": "string"},
                "title": {"type": "string"},
            },
        },
    },
    {
        "name": "end_session",
        "description": "Close a session, optionally append final events, and enqueue a consolidation job.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "session_id"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "session_id": {"type": "string"},
                "final_events": {"type": "array", "items": {"type": "object"}},
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
                "expected_revision": {"type": "integer"},
                "reason": {"type": "string"},
            },
        },
    },
    {
        "name": "forget_record",
        "description": "Explicitly delete a record or event, clear derived copies, and record a content-free tombstone.",
        "inputSchema": {
            "type": "object",
            "required": ["context_id", "idempotency_key", "record_id", "reason", "confirm_forget"],
            "properties": {
                "context_id": {"type": "string"},
                "idempotency_key": {"type": "string"},
                "record_id": {"type": "string"},
                "expected_revision": {"type": "integer"},
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
