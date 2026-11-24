"""Unified Application Core implementing all 13 logical operations (Design Section 9)."""

from __future__ import annotations

import base64
import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .access import AccessPolicy, ClientIdentity, intersect_scopes, require_scope
from .bootstrap import init_memory_home
from .config import MemoryConfig
from .consolidation import ConsolidationProvider, StubConsolidationProvider
from .errors import MemoryServiceError
from .frontmatter import content_hash, parse_markdown, render_markdown
from .ids import canonical_request_hash, new_id, sha256_bytes, sha256_text, utc_now, utc_now_iso
from .index import MemoryIndex, scope_permitted
from .journal import FileMutationOp, MutationJournal
from .schemas import SchemaRegistry
from .scopes import AreaRegistry, normalize_scope_id, sanitize_subpath, segment_to_folder_name
from .storage import atomic_write_bytes, atomic_write_json, atomic_write_text, ensure_within, event_record_path, record_vault_path
from .tasks import (
    check_dependency_cycles,
    render_area_tasks_markdown,
    render_today_markdown,
    validate_task_transition,
)
from .views import build_bootstrap_packet, render_owner_summary_markdown

MEMORY_UPDATE_ALLOWLIST = frozenset(
    {"title", "body", "status", "valid_from", "valid_until", "occurred_at", "relations", "sources"}
)
TASK_UPDATE_ALLOWLIST = frozenset(
    {
        "title",
        "body",
        "status",
        "priority",
        "scheduled_date",
        "due_date",
        "blocked_reason",
        "depends_on",
        "relations",
        "sources",
    }
)
MUTATING_OPERATIONS = frozenset(
    {
        "start_session",
        "append_events",
        "create_memory",
        "update_memory",
        "create_task",
        "update_task",
        "ingest_document",
        "end_session",
        "archive_record",
        "forget_record",
        "register_area",
    }
)


class MemoryCore:
    """Single-process application core shared by HTTP, MCP, CLI, and import adapters."""

    def __init__(
        self,
        config: MemoryConfig,
        *,
        provider: ConsolidationProvider | None = None,
        auto_init: bool = True,
    ) -> None:
        self.config = config
        if auto_init:
            init_memory_home(config)
        self.areas = AreaRegistry(config)
        self.schemas = SchemaRegistry(config.schemas_dir, fallback_dir=config.repo_schemas_dir)
        self.access = AccessPolicy(config)
        self.journal = MutationJournal(config)
        self.index = MemoryIndex(config)
        self.provider: ConsolidationProvider = provider or StubConsolidationProvider()

        # Startup crash recovery before serving content (Design Section 10)
        recovery = self.journal.recover_incomplete()
        if recovery["recovered"] or recovery["needs_projection"]:
            self.index.rebuild_from_vault(self.config.vault_dir)
            self.refresh_derived_views()
            for mut_id in recovery["needs_projection"]:
                self.journal.mark_completed(mut_id)

    def close(self) -> None:
        self.index.close()

    # -- Unified Logical Dispatcher -------------------------------------------

    def dispatch(
        self,
        *,
        client: ClientIdentity,
        operation: str,
        payload: dict[str, Any] | None = None,
        context_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """Execute a logical operation and return the Section 9.1 response envelope."""
        request_id = new_id("req")
        inp = payload or {}
        warnings: list[str] = []

        try:
            if not isinstance(inp, dict):
                raise MemoryServiceError("validation_failed", "Operation input must be a JSON object")

            # Enforce write permission for mutating operations
            if operation in MUTATING_OPERATIONS:
                if not client.can_write:
                    raise MemoryServiceError("authentication_required", "Client credential is read-only")
                if not idempotency_key or not isinstance(idempotency_key, str) or not idempotency_key.strip():
                    raise MemoryServiceError(
                        "validation_failed",
                        f"idempotency_key is mandatory for operation '{operation}'",
                    )
                req_hash = canonical_request_hash(operation, inp)
                existing = self.index.get_idempotency(client.client_id, idempotency_key)
                if existing is not None:
                    if existing["request_hash"] != req_hash or existing["operation"] != operation:
                        raise MemoryServiceError(
                            "idempotency_conflict",
                            "Idempotency key already used with a different operation or payload",
                        )
                    return json.loads(existing["response_json"])

            if operation == "open_context":
                data = self.open_context(client=client, payload=inp)
            else:
                ctx = self._require_context(client, context_id)
                handler = self._HANDLERS.get(operation)
                if handler is None:
                    raise MemoryServiceError("validation_failed", f"Unknown operation: {operation}")
                data = handler(self, client=client, ctx=ctx, payload=inp, warnings=warnings)

            response = {
                "request_id": request_id,
                "ok": True,
                "data": data,
                "policy_version": self.config.policy_version,
                "warnings": warnings,
            }
            if operation in MUTATING_OPERATIONS and idempotency_key:
                self.index.save_idempotency(
                    client_id=client.client_id,
                    key=idempotency_key,
                    operation=operation,
                    request_hash=canonical_request_hash(operation, inp),
                    response=response,
                )
            return response

        except MemoryServiceError as exc:
            return {
                "request_id": request_id,
                "ok": False,
                "error": exc.to_dict(),
                "policy_version": self.config.policy_version,
                "warnings": warnings,
            }

    # -- Context & Access -----------------------------------------------------

    def _require_context(self, client: ClientIdentity, context_id: str | None) -> dict[str, Any]:
        if not context_id:
            raise MemoryServiceError("context_required", "Active context_id from open_context is required")
        ctx = self.index.get_context(context_id)
        if ctx is None or ctx["client_id"] != client.client_id:
            raise MemoryServiceError("context_required", "Unknown or unauthorized context_id")
        expires = datetime.fromisoformat(ctx["expires_at"].replace("Z", "+00:00"))
        if utc_now() >= expires:
            raise MemoryServiceError("context_expired", "Application context has expired; call open_context again")
        if ctx["policy_version"] != self.config.policy_version:
            raise MemoryServiceError(
                "policy_refresh_required",
                "Policy version changed; call open_context to refresh operating rules",
            )
        return ctx

    def open_context(self, *, client: ClientIdentity, payload: dict[str, Any]) -> dict[str, Any]:
        purpose = str(payload.get("purpose") or "").strip()
        if not purpose:
            raise MemoryServiceError("validation_failed", "open_context requires a non-empty 'purpose'")
        requested_scopes = payload.get("scopes")
        if requested_scopes is not None and not isinstance(requested_scopes, list):
            raise MemoryServiceError("validation_failed", "'scopes' must be a list of scope IDs")
        project_id = payload.get("project_id")

        effective_scopes = intersect_scopes(client.allowed_scopes, requested_scopes)
        if not effective_scopes:
            raise MemoryServiceError(
                "validation_failed",
                "Requested scopes have no intersection with credential permissions",
            )

        now = utc_now()
        expires = now + timedelta(seconds=self.config.context_ttl_seconds)
        ctx = {
            "context_id": new_id("ctx"),
            "client_id": client.client_id,
            "allowed_scopes": effective_scopes,
            "purpose": purpose,
            "project_id": project_id,
            "policy_version": self.config.policy_version,
            "created_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "expires_at": expires.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        self.index.save_context(ctx)

        packet = build_bootstrap_packet(
            config=self.config,
            index=self.index,
            allowed_scopes=effective_scopes,
            project_id=project_id,
        )
        return {
            "context_id": ctx["context_id"],
            "allowed_scopes": effective_scopes,
            "policy_version": ctx["policy_version"],
            "expires_at": ctx["expires_at"],
            "bootstrap": packet,
        }

    # -- Sessions & Event Capture ---------------------------------------------

    def start_session(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        app_label = str(payload.get("source_application") or "").strip()
        coverage = str(payload.get("capture_coverage") or "").strip()
        scope_id = str(payload.get("scope_id") or ctx["allowed_scopes"][0]).strip().lower()
        project_id = payload.get("project_id") or ctx.get("project_id")
        known_gaps = payload.get("known_gaps") or []

        if not app_label:
            raise MemoryServiceError("validation_failed", "start_session requires 'source_application'")
        require_scope(scope_id, ctx["allowed_scopes"])

        ses_id = new_id("ses")
        now = utc_now_iso()
        title = str(payload.get("title") or f"Session {app_label} {now[:10]}")
        meta = {
            "schema_version": "1.0",
            "id": ses_id,
            "type": "session",
            "context_id": ctx["context_id"],
            "source_application": app_label,
            "scope_id": scope_id,
            "project_id": project_id,
            "title": title,
            "revision": 1,
            "started_at": now,
            "ended_at": None,
            "capture_coverage": coverage,
            "known_gaps": known_gaps,
            "processing_checkpoint": 0,
            "handoff_summary_id": None,
        }
        self.schemas.validate("session", meta)

        body = (
            f"# {title}\n\n"
            f"## Capture Coverage\n- Coverage: `{coverage}`\n"
            f"- Source application: `{app_label}`\n"
        )
        rendered = render_markdown(meta, body)
        target_path = self._resolve_record_path(
            kind="session", scope_id=scope_id, record_id=ses_id, title=title, folder=payload.get("folder")
        )
        entry = self.journal.execute(
            operation="start_session",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"session_id": ses_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=ses_id,
            kind="session",
            meta=meta,
            body=body,
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.journal.mark_completed(entry["mutation_id"])
        return {
            "session_id": ses_id,
            "scope_id": scope_id,
            "capture_coverage": coverage,
            "revision": 1,
            "mutation_id": entry["mutation_id"],
        }

    def append_events(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        session_id = str(payload.get("session_id") or "").strip()
        events = payload.get("events")
        if not session_id:
            raise MemoryServiceError("validation_failed", "append_events requires 'session_id'")
        if not isinstance(events, list) or not events:
            raise MemoryServiceError("validation_failed", "append_events requires a non-empty 'events' array")
        if len(events) > self.config.max_event_batch_size:
            raise MemoryServiceError(
                "validation_failed",
                f"Event batch exceeds max size ({self.config.max_event_batch_size})",
            )

        ses_rec = self.index.get_record(session_id)
        if ses_rec is None or ses_rec["kind"] != "session":
            raise MemoryServiceError("not_found_or_unavailable", "Session not found or unavailable")
        require_scope(ses_rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        accepted_ids: list[str] = []
        duplicate_ids: list[str] = []
        skipped_tombstoned: list[str] = []

        with self.journal.lock:
            existing_count = len(self.index.list_session_events(session_id))
            for idx, item in enumerate(events):
                if not isinstance(item, dict):
                    raise MemoryServiceError("validation_failed", f"Event at index {idx} must be an object")
                content = str(item.get("content") or "")
                if len(content) > self.config.max_body_chars:
                    raise MemoryServiceError("validation_failed", "Event content exceeds max length")

                source_event_id = item.get("source_event_id")
                speaker = str(item.get("speaker") or "")
                event_type = str(item.get("event_type") or "message")
                c_hash = sha256_text(f"{speaker}|{event_type}|{content}")

                # Check tombstone reimport prevention
                if source_event_id and self.index.is_tombstoned(fingerprint=f"src:{client.client_id}:{source_event_id}"):
                    skipped_tombstoned.append(str(source_event_id))
                    warnings.append(f"Skipped previously forgotten source_event_id '{source_event_id}'")
                    continue

                # Deduplication by (client_id, source_event_id)
                if source_event_id:
                    prior = self.index.find_event_by_source(client.client_id, str(source_event_id))
                    if prior is not None:
                        if prior["content_hash"] != c_hash:
                            raise MemoryServiceError(
                                "source_conflict",
                                f"source_event_id '{source_event_id}' already exists with different content",
                            )
                        duplicate_ids.append(prior["id"])
                        continue

                evt_id = new_id("evt")
                recorded_at = utc_now_iso()
                evt_doc = {
                    "schema_version": "1.0",
                    "id": evt_id,
                    "session_id": session_id,
                    "scope_id": ses_rec["scope_id"],
                    "source_application": ses_rec["metadata"]["source_application"],
                    "source_event_id": str(source_event_id) if source_event_id is not None else None,
                    "sequence": int(item.get("sequence", existing_count + idx)),
                    "speaker": speaker,
                    "event_type": event_type,
                    "recorded_at": recorded_at,
                    "occurred_at": item.get("occurred_at"),
                    "content": content,
                    "attachment_ids": list(item.get("attachment_ids") or []),
                    "source_reference": item.get("source_reference"),
                }
                self.schemas.validate("event", evt_doc)

                # Persist immutable original event before acknowledging capture
                evt_path = event_record_path(
                    self.config, session_id=session_id, event_id=evt_id, recorded_at=recorded_at
                )
                atomic_write_json(evt_path, evt_doc)
                self.index.insert_event(evt_doc, client_id=client.client_id, c_hash=c_hash, file_path=evt_path)
                accepted_ids.append(evt_id)

        return {
            "session_id": session_id,
            "accepted_ids": accepted_ids,
            "duplicate_ids": duplicate_ids,
            "skipped_tombstoned": skipped_tombstoned,
        }

    def end_session(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        session_id = str(payload.get("session_id") or "").strip()
        if not session_id:
            raise MemoryServiceError("validation_failed", "end_session requires 'session_id'")

        final_events = payload.get("final_events")
        final_capture: dict[str, Any] | None = None
        if final_events:
            final_capture = self.append_events(
                client=client,
                ctx=ctx,
                payload={"session_id": session_id, "events": final_events},
                warnings=warnings,
            )

        ses_rec = self.index.get_record(session_id)
        if ses_rec is None or ses_rec["kind"] != "session":
            raise MemoryServiceError("not_found_or_unavailable", "Session not found or unavailable")
        require_scope(ses_rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        meta = dict(ses_rec["metadata"])
        meta["ended_at"] = utc_now_iso()
        meta["revision"] = int(meta["revision"]) + 1
        self.schemas.validate("session", meta)

        rendered = render_markdown(meta, ses_rec["body"])
        target_path = Path(ses_rec["file_path"])
        entry = self.journal.execute(
            operation="end_session",
            ops=[
                FileMutationOp(
                    path=target_path,
                    action="write",
                    after_content=rendered,
                    expected_before_hash=ses_rec["content_hash"],
                )
            ],
            metadata={"session_id": session_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=session_id,
            kind="session",
            meta=meta,
            body=ses_rec["body"],
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        job = self.index.enqueue_job(
            job_id=new_id("job"),
            job_type="consolidate_session",
            payload={"session_id": session_id, "scope_id": ses_rec["scope_id"]},
        )
        self.journal.mark_completed(entry["mutation_id"])
        return {
            "session_id": session_id,
            "ended_at": meta["ended_at"],
            "final_capture": final_capture,
            "consolidation_job_id": job["job_id"],
        }

    # -- Retrieval ------------------------------------------------------------

    def search_memory(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        query = str(payload.get("query") or "")
        kind = payload.get("kind")
        include_archived = bool(payload.get("include_archived", False))
        limit = min(int(payload.get("limit", 20)), self.config.max_search_limit)
        if limit < 1:
            raise MemoryServiceError("validation_failed", "limit must be >= 1")

        filter_scopes = payload.get("scopes")
        effective_scopes = intersect_scopes(ctx["allowed_scopes"], filter_scopes) if filter_scopes else ctx["allowed_scopes"]
        if not effective_scopes:
            return {"results": [], "total": 0}

        rows = self.index.search_records(
            query=query,
            permitted_scopes=effective_scopes,
            kind=kind,
            include_archived=include_archived,
            limit=limit,
        )
        results = [
            {
                "id": r["id"],
                "kind": r["kind"],
                "type": r["record_type"],
                "scope_id": r["scope_id"],
                "title": r["title"],
                "status": r["status"],
                "revision": r["revision"],
                "excerpt": r.get("excerpt", ""),
                "sources": r["metadata"].get("sources", []),
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]
        return {"results": results, "total": len(results)}

    def read_page(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        record_id = str(payload.get("record_id") or "").strip()
        if not record_id:
            raise MemoryServiceError("validation_failed", "read_page requires 'record_id'")
        max_chars = int(payload.get("max_chars", self.config.max_body_chars))

        rec = self.index.get_record(record_id)
        if rec is None:
            raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
        require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        # Filter related records strictly by scope so inaccessible targets never leak titles/paths
        raw_rels = self.index.get_relations(record_id)
        permitted_rels: list[dict[str, Any]] = []
        for rel in raw_rels:
            other_id = rel["target_id"] if rel["source_id"] == record_id else rel["source_id"]
            other = self.index.get_record(other_id)
            if other is not None and scope_permitted(other["scope_id"], ctx["allowed_scopes"]):
                permitted_rels.append(
                    {
                        "type": rel["rel_type"],
                        "direction": "outbound" if rel["source_id"] == record_id else "inbound",
                        "record_id": other["id"],
                        "title": other["title"],
                        "scope_id": other["scope_id"],
                    }
                )

        body = rec["body"][:max_chars]
        return {
            "id": rec["id"],
            "kind": rec["kind"],
            "scope_id": rec["scope_id"],
            "revision": rec["revision"],
            "metadata": rec["metadata"],
            "body": body,
            "truncated": len(rec["body"]) > max_chars,
            "related": permitted_rels,
        }

    # -- Memories -------------------------------------------------------------

    def create_memory(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        scope_id = str(payload.get("scope_id") or "").strip().lower()
        require_scope(scope_id, ctx["allowed_scopes"])

        title = str(payload.get("title") or "").strip()
        body = str(payload.get("body") or "").strip()
        if not title or not body:
            raise MemoryServiceError("validation_failed", "create_memory requires non-empty 'title' and 'body'")
        if len(body) > self.config.max_body_chars:
            raise MemoryServiceError("validation_failed", "Memory body exceeds max length")

        origin = str(payload.get("origin") or "")
        status = str(payload.get("status") or ("uncertain" if origin == "inference" else "current"))
        if origin == "inference" and status == "current":
            status = "uncertain"
            warnings.append("Inference origin downgraded status from 'current' to 'uncertain' per memory policy")

        sources = payload.get("sources") or []
        self._validate_sources(sources)

        mem_id = new_id("mem")
        now = utc_now_iso()
        meta: dict[str, Any] = {
            "schema_version": "1.0",
            "id": mem_id,
            "type": str(payload.get("type") or ""),
            "scope_id": scope_id,
            "project_id": payload.get("project_id"),
            "title": title,
            "revision": 1,
            "status": status,
            "origin": origin,
            "recorded_at": now,
            "updated_at": now,
            "occurred_at": payload.get("occurred_at"),
            "valid_from": payload.get("valid_from"),
            "valid_until": payload.get("valid_until"),
            "sources": sources,
            "relations": list(payload.get("relations") or []),
            "processing": payload.get("processing"),
            "archived": False,
        }
        self.schemas.validate("memory", meta)

        full_body = f"# {title}\n\n{body}" if not body.startswith("# ") else body
        rendered = render_markdown(meta, full_body)
        target_path = self._resolve_record_path(
            kind="memory", scope_id=scope_id, record_id=mem_id, title=title, folder=payload.get("folder")
        )
        entry = self.journal.execute(
            operation="create_memory",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"record_id": mem_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=mem_id,
            kind="memory",
            meta=meta,
            body=full_body,
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.refresh_derived_views()
        self.journal.mark_completed(entry["mutation_id"])
        return {"record_id": mem_id, "revision": 1, "mutation_id": entry["mutation_id"], "status": status}

    def update_memory(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        record_id = str(payload.get("record_id") or "").strip()
        expected_rev = payload.get("expected_revision")
        changes = payload.get("changes")
        reason = str(payload.get("reason") or "").strip()
        source_ids = payload.get("source_ids") or []

        if not record_id or not isinstance(expected_rev, int) or not isinstance(changes, dict) or not changes:
            raise MemoryServiceError(
                "validation_failed",
                "update_memory requires 'record_id', integer 'expected_revision', and non-empty 'changes'",
            )
        if not reason:
            raise MemoryServiceError("validation_failed", "update_memory requires a non-empty 'reason'")

        unknown_fields = set(changes.keys()) - MEMORY_UPDATE_ALLOWLIST
        if unknown_fields:
            raise MemoryServiceError(
                "validation_failed",
                f"Disallowed fields in update_memory: {sorted(unknown_fields)}",
            )

        rec = self.index.get_record(record_id)
        if rec is None or rec["kind"] != "memory":
            raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
        require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        if rec["revision"] != expected_rev:
            raise MemoryServiceError(
                "revision_conflict",
                f"Expected revision {expected_rev}, but current revision is {rec['revision']}",
                details={"current_revision": rec["revision"]},
            )

        meta = dict(rec["metadata"])
        body = rec["body"]
        for key, val in changes.items():
            if key == "body":
                body = str(val)
            else:
                meta[key] = val

        if source_ids:
            existing_sources = list(meta.get("sources") or [])
            seen_ids = {s.get("record_id") for s in existing_sources}
            for sid in source_ids:
                if sid not in seen_ids:
                    existing_sources.append({"record_id": str(sid)})
                    seen_ids.add(sid)
            meta["sources"] = existing_sources

        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = utc_now_iso()
        self.schemas.validate("memory", meta)

        rendered = render_markdown(meta, body)
        target_path = Path(rec["file_path"])
        entry = self.journal.execute(
            operation="update_memory",
            ops=[
                FileMutationOp(
                    path=target_path,
                    action="write",
                    after_content=rendered,
                    expected_before_hash=rec["content_hash"],
                )
            ],
            metadata={"record_id": record_id, "reason": reason},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=record_id,
            kind="memory",
            meta=meta,
            body=body,
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.refresh_derived_views()
        self.journal.mark_completed(entry["mutation_id"])
        return {"record_id": record_id, "revision": meta["revision"], "mutation_id": entry["mutation_id"]}

    # -- Tasks ----------------------------------------------------------------

    def create_task(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        scope_id = str(payload.get("scope_id") or "").strip().lower()
        require_scope(scope_id, ctx["allowed_scopes"])

        title = str(payload.get("title") or "").strip()
        if not title:
            raise MemoryServiceError("validation_failed", "create_task requires a non-empty 'title'")

        origin = str(payload.get("origin") or "explicit_user_statement")
        is_proposal = bool(payload.get("proposal", False)) or origin == "assistant_proposal"
        status = "inbox" if is_proposal else str(payload.get("status") or "inbox")
        if is_proposal and payload.get("status") and payload.get("status") != "inbox":
            warnings.append("Assistant-suggested tasks must enter 'inbox' as proposals")

        priority = str(payload.get("priority") or "normal")
        sources = payload.get("sources") or []
        self._validate_sources(sources)

        task_id = new_id("task")
        depends_on = list(payload.get("depends_on") or [])
        if depends_on:
            all_dep_map = {
                t["id"]: list(t["metadata"].get("depends_on") or [])
                for t in self.index.list_records(kind="task", include_archived=True)
            }
            check_dependency_cycles(task_id, depends_on, all_dep_map)

        now = utc_now_iso()
        meta: dict[str, Any] = {
            "schema_version": "1.0",
            "id": task_id,
            "type": "task",
            "scope_id": scope_id,
            "project_id": payload.get("project_id"),
            "title": title,
            "revision": 1,
            "status": status,
            "priority": priority,
            "origin": origin,
            "proposal": is_proposal,
            "scheduled_date": payload.get("scheduled_date"),
            "due_date": payload.get("due_date"),
            "blocked_reason": payload.get("blocked_reason"),
            "depends_on": depends_on,
            "completed_at": None,
            "completion_history": [],
            "recorded_at": now,
            "updated_at": now,
            "sources": sources,
            "relations": list(payload.get("relations") or []),
            "archived": False,
        }
        self.schemas.validate("task", meta)

        body = str(payload.get("body") or f"# {title}\n\n## Notes\nNo deadline has been stated.")
        if not body.startswith("# "):
            body = f"# {title}\n\n{body}"

        rendered = render_markdown(meta, body)
        target_path = self._resolve_record_path(
            kind="task", scope_id=scope_id, record_id=task_id, title=title, folder=payload.get("folder")
        )
        entry = self.journal.execute(
            operation="create_task",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"record_id": task_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=task_id,
            kind="task",
            meta=meta,
            body=body,
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.refresh_derived_views()
        self.journal.mark_completed(entry["mutation_id"])
        return {
            "record_id": task_id,
            "revision": 1,
            "status": status,
            "proposal": is_proposal,
            "mutation_id": entry["mutation_id"],
        }

    def update_task(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        record_id = str(payload.get("record_id") or "").strip()
        expected_rev = payload.get("expected_revision")
        changes = payload.get("changes")
        source_ids = payload.get("source_ids") or []
        reason = payload.get("reason")
        explicit_user_instruction = bool(payload.get("explicit_user_instruction", False))

        if not record_id or not isinstance(expected_rev, int) or not isinstance(changes, dict) or not changes:
            raise MemoryServiceError(
                "validation_failed",
                "update_task requires 'record_id', integer 'expected_revision', and non-empty 'changes'",
            )
        unknown_fields = set(changes.keys()) - TASK_UPDATE_ALLOWLIST
        if unknown_fields:
            raise MemoryServiceError(
                "validation_failed",
                f"Disallowed fields in update_task: {sorted(unknown_fields)}",
            )

        rec = self.index.get_record(record_id)
        if rec is None or rec["kind"] != "task":
            raise MemoryServiceError("not_found_or_unavailable", "Task not found or unavailable")
        require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        if rec["revision"] != expected_rev:
            raise MemoryServiceError(
                "revision_conflict",
                f"Expected revision {expected_rev}, but current revision is {rec['revision']}",
                details={"current_revision": rec["revision"]},
            )

        meta = dict(rec["metadata"])
        body = rec["body"]
        old_status = meta["status"]

        for key, val in changes.items():
            if key == "body":
                body = str(val)
            else:
                meta[key] = val

        new_status = meta["status"]
        new_deps = list(meta.get("depends_on") or [])

        # Dependency cycle check & unresolved dependency check
        all_tasks = self.index.list_records(kind="task", include_archived=True)
        dep_map = {t["id"]: list(t["metadata"].get("depends_on") or []) for t in all_tasks}
        check_dependency_cycles(record_id, new_deps, dep_map)
        unresolved = {
            t["id"] for t in all_tasks if t["metadata"].get("status") not in ("done", "cancelled")
        }

        validate_task_transition(
            current_status=old_status,
            new_status=new_status,
            blocked_reason=meta.get("blocked_reason"),
            depends_on=new_deps,
            unresolved_dep_ids=unresolved,
            has_completion_evidence=bool(source_ids) or explicit_user_instruction,
            reopen_reason=str(reason) if reason else None,
        )

        now = utc_now_iso()
        history = list(meta.get("completion_history") or [])
        if new_status == "done" and old_status != "done":
            meta["completed_at"] = now
            history.append({"action": "completed", "at": now, "reason": str(reason) if reason else None})
        elif new_status == "cancelled" and old_status != "cancelled":
            history.append({"action": "cancelled", "at": now, "reason": str(reason) if reason else None})
        elif old_status in ("done", "cancelled") and new_status == "next":
            meta["completed_at"] = None
            history.append({"action": "reopened", "at": now, "reason": str(reason)})
        meta["completion_history"] = history

        if source_ids:
            existing_sources = list(meta.get("sources") or [])
            seen = {s.get("record_id") for s in existing_sources}
            for sid in source_ids:
                if sid not in seen:
                    existing_sources.append({"record_id": str(sid)})
                    seen.add(sid)
            meta["sources"] = existing_sources

        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = now
        self.schemas.validate("task", meta)

        rendered = render_markdown(meta, body)
        target_path = Path(rec["file_path"])
        entry = self.journal.execute(
            operation="update_task",
            ops=[
                FileMutationOp(
                    path=target_path,
                    action="write",
                    after_content=rendered,
                    expected_before_hash=rec["content_hash"],
                )
            ],
            metadata={"record_id": record_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=record_id,
            kind="task",
            meta=meta,
            body=body,
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.refresh_derived_views()
        self.journal.mark_completed(entry["mutation_id"])
        return {
            "record_id": record_id,
            "revision": meta["revision"],
            "status": new_status,
            "mutation_id": entry["mutation_id"],
        }

    # -- Document Ingestion ---------------------------------------------------

    def ingest_document(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        scope_id = str(payload.get("scope_id") or "").strip().lower()
        require_scope(scope_id, ctx["allowed_scopes"])

        original_filename = Path(str(payload.get("original_filename") or "")).name
        media_type = str(payload.get("media_type") or "application/octet-stream").strip()
        content_b64 = payload.get("content_base64")
        if not original_filename or not isinstance(content_b64, str):
            raise MemoryServiceError(
                "validation_failed",
                "ingest_document requires 'original_filename' and 'content_base64'",
            )
        try:
            raw_bytes = base64.b64decode(content_b64, validate=True)
        except Exception as exc:
            raise MemoryServiceError("validation_failed", f"Invalid base64 payload: {exc}") from exc

        if len(raw_bytes) > self.config.max_attachment_bytes:
            raise MemoryServiceError(
                "validation_failed",
                f"Attachment exceeds max size ({self.config.max_attachment_bytes} bytes)",
            )

        checksum = sha256_bytes(raw_bytes)
        doc_id = new_id("doc")
        now = utc_now_iso()
        safe_ext = "".join(ch for ch in Path(original_filename).suffix if ch.isalnum() or ch == ".")[:12]
        att_rel = Path(scope_id.replace("/", "_")) / f"{doc_id}{safe_ext}"
        att_path = ensure_within(self.config.attachments_dir, self.config.attachments_dir / att_rel)
        atomic_write_bytes(att_path, raw_bytes)

        title = str(payload.get("title") or Path(original_filename).stem or doc_id)
        meta: dict[str, Any] = {
            "schema_version": "1.0",
            "id": doc_id,
            "type": "document",
            "scope_id": scope_id,
            "project_id": payload.get("project_id"),
            "title": title,
            "revision": 1,
            "original_attachment_path": str(att_path.relative_to(self.config.records_dir)),
            "original_filename": original_filename,
            "media_type": media_type,
            "checksum_sha256": checksum,
            "size_bytes": len(raw_bytes),
            "acquisition_source": payload.get("acquisition_source"),
            "acquired_at": now,
            "extraction_status": "pending",
            "page_count": None,
            "derived_note_ids": [],
            "archived": False,
        }
        self.schemas.validate("document", meta)

        body = (
            f"# {title}\n\n"
            f"## Metadata\n"
            f"- Original filename: `{original_filename}`\n"
            f"- Media type: `{media_type}`\n"
            f"- Checksum (SHA-256): `{checksum}`\n"
            f"- Extraction status: `pending`\n"
        )
        rendered = render_markdown(meta, body)
        companion_path = self._resolve_record_path(
            kind="document", scope_id=scope_id, record_id=doc_id, title=title, folder=payload.get("folder")
        )
        entry = self.journal.execute(
            operation="ingest_document",
            ops=[FileMutationOp(path=companion_path, action="write", after_content=rendered)],
            metadata={"record_id": doc_id},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=doc_id,
            kind="document",
            meta=meta,
            body=body,
            file_path=companion_path,
            raw_hash=content_hash(rendered),
        )
        job = self.index.enqueue_job(
            job_id=new_id("job"),
            job_type="extract_document",
            payload={"document_id": doc_id, "scope_id": scope_id, "media_type": media_type},
        )
        self.journal.mark_completed(entry["mutation_id"])
        return {
            "document_id": doc_id,
            "checksum_sha256": checksum,
            "extraction_status": "pending",
            "extraction_job_id": job["job_id"],
            "mutation_id": entry["mutation_id"],
        }

    # -- Archive & Forget -----------------------------------------------------

    def archive_record(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        record_id = str(payload.get("record_id") or "").strip()
        expected_rev = payload.get("expected_revision")
        reason = str(payload.get("reason") or "").strip()
        if not record_id or not isinstance(expected_rev, int) or not reason:
            raise MemoryServiceError(
                "validation_failed",
                "archive_record requires 'record_id', integer 'expected_revision', and 'reason'",
            )

        rec = self.index.get_record(record_id)
        if rec is None:
            raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
        require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        if rec["revision"] != expected_rev:
            raise MemoryServiceError(
                "revision_conflict",
                f"Expected revision {expected_rev}, but current revision is {rec['revision']}",
                details={"current_revision": rec["revision"]},
            )

        meta = dict(rec["metadata"])
        meta["archived"] = True
        meta["archived_at"] = utc_now_iso()
        meta["archived_reason"] = reason
        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = utc_now_iso()

        rendered = render_markdown(meta, rec["body"])
        target_path = Path(rec["file_path"])
        entry = self.journal.execute(
            operation="archive_record",
            ops=[
                FileMutationOp(
                    path=target_path,
                    action="write",
                    after_content=rendered,
                    expected_before_hash=rec["content_hash"],
                )
            ],
            metadata={"record_id": record_id, "reason": reason},
        )
        self.journal.mark_projecting(entry["mutation_id"])
        self.index.upsert_record(
            record_id=record_id,
            kind=rec["kind"],
            meta=meta,
            body=rec["body"],
            file_path=target_path,
            raw_hash=content_hash(rendered),
        )
        self.refresh_derived_views()
        self.journal.mark_completed(entry["mutation_id"])
        return {"record_id": record_id, "archived": True, "revision": meta["revision"]}

    def forget_record(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        if not client.can_forget:
            raise MemoryServiceError("authentication_required", "Client credential is not permitted to forget records")
        record_id = str(payload.get("record_id") or "").strip()
        expected_rev = payload.get("expected_revision")
        reason = str(payload.get("reason") or "").strip()
        confirm = payload.get("confirm_forget") is True
        if not record_id or not reason or not confirm:
            raise MemoryServiceError(
                "validation_failed",
                "forget_record requires 'record_id', 'reason', and explicit 'confirm_forget': true",
            )

        # Support forgetting either a vault record or an original event
        rec = self.index.get_record(record_id)
        if rec is not None:
            require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)
            if not isinstance(expected_rev, int) or rec["revision"] != expected_rev:
                raise MemoryServiceError(
                    "revision_conflict",
                    f"Expected revision {expected_rev}, but current revision is {rec['revision']}",
                    details={"current_revision": rec["revision"]},
                )
            ops = [
                FileMutationOp(
                    path=Path(rec["file_path"]),
                    action="delete",
                    expected_before_hash=rec["content_hash"],
                )
            ]
            entry = self.journal.execute(
                operation="forget_record",
                ops=ops,
                metadata={"record_id": record_id},
            )
            self.journal.mark_projecting(entry["mutation_id"])
            self.index.delete_record(record_id)
            self.index.add_tombstone(
                record_id=record_id,
                kind=rec["kind"],
                scope_id=rec["scope_id"],
                fingerprints=[f"id:{record_id}"],
                reason=reason,
            )
            self.refresh_derived_views()
            self.journal.mark_completed(entry["mutation_id"])
            return {
                "record_id": record_id,
                "forgotten": True,
                "tombstone_recorded": True,
                "derived_cleanup": "active indexes, FTS5, relations, and derived views refreshed",
                "limitations": (
                    "Offline backups and any external provider logs previously sent are not retroactively "
                    "erased by this local deletion."
                ),
            }

        evt = self.index.get_event(record_id)
        if evt is not None:
            require_scope(evt["scope_id"], ctx["allowed_scopes"], hide_existence=True)
            evt_path = Path(evt["file_path"])
            if evt_path.exists():
                evt_path.unlink()
            fingerprints = [f"id:{record_id}"]
            if evt.get("source_event_id"):
                fingerprints.append(f"src:{evt['client_id']}:{evt['source_event_id']}")
            self.index.delete_event_index(record_id)
            self.index.add_tombstone(
                record_id=record_id,
                kind="event",
                scope_id=evt["scope_id"],
                fingerprints=fingerprints,
                reason=reason,
            )
            return {
                "record_id": record_id,
                "forgotten": True,
                "tombstone_recorded": True,
                "derived_cleanup": "event file and index entry removed; reimport blocked by tombstone",
                "limitations": (
                    "Offline backups and any external provider logs previously sent are not retroactively "
                    "erased by this local deletion."
                ),
            }

        raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")

    # -- Dynamic Areas & Projections ------------------------------------------

    def _resolve_record_path(
        self, *, kind: str, scope_id: str, record_id: str, title: str, folder: str | None = None
    ) -> Path:
        norm = normalize_scope_id(scope_id)
        top = norm.split("/", 1)[0]
        area_folder = self.areas.get_area_folder(top)
        custom_subpath = sanitize_subpath(folder) if folder else None
        return record_vault_path(
            self.config,
            kind=kind,
            scope_id=norm,
            record_id=record_id,
            title=title,
            area_folder=area_folder,
            custom_subpath=custom_subpath,
        )

    def list_areas(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        all_areas = self.areas.list_areas()
        permitted = {
            slug: cfg for slug, cfg in all_areas.items() if scope_permitted(slug, ctx["allowed_scopes"])
        }
        return {"areas": permitted}

    def register_area(
        self, *, client: ClientIdentity, ctx: dict[str, Any], payload: dict[str, Any], warnings: list[str]
    ) -> dict[str, Any]:
        slug = normalize_scope_id(str(payload.get("slug") or ""))
        if "/" in slug:
            raise MemoryServiceError("validation_failed", "register_area expects a top-level area slug without '/'")
        require_scope(slug, ctx["allowed_scopes"])
        subfolders = payload.get("subfolders")
        if subfolders is not None and not isinstance(subfolders, list):
            raise MemoryServiceError("validation_failed", "'subfolders' must be a list of folder names")
        entry = self.areas.register_area(
            slug=slug,
            folder=payload.get("folder"),
            description=str(payload.get("description") or ""),
            subfolders=[str(s) for s in subfolders] if subfolders is not None else None,
            has_tasks_dashboard=bool(payload.get("has_tasks_dashboard", True)),
            create_in_vault=True,
        )
        self.refresh_derived_views()
        return entry

    def refresh_derived_views(self) -> None:
        """Regenerate TODAY.md, dynamic per-area TASKS.md dashboards, and SUMMARY.md."""
        all_tasks = self.index.list_records(kind="task", include_archived=False)
        today_md = render_today_markdown(all_tasks, timezone=self.config.timezone)
        atomic_write_text(self.config.vault_dir / "TODAY.md", today_md)

        # Dynamically generate TASKS.md for every registered area (and any ad-hoc area with tasks)
        areas = self.areas.list_areas()
        dashboard_areas: dict[str, str] = {}
        for slug, cfg in areas.items():
            if cfg.get("has_tasks_dashboard", slug != "shared"):
                dashboard_areas[slug] = str(cfg["folder"])
        for task in all_tasks:
            top = task["scope_id"].split("/", 1)[0].strip().lower()
            if top not in dashboard_areas and top != "shared":
                dashboard_areas[top] = self.areas.get_area_folder(top)

        for slug, folder_name in dashboard_areas.items():
            area_tasks = [t for t in all_tasks if scope_permitted(t["scope_id"], [slug])]
            area_dir = ensure_within(self.config.vault_dir, self.config.vault_dir / folder_name)
            area_dir.mkdir(parents=True, exist_ok=True)
            atomic_write_text(area_dir / "TASKS.md", render_area_tasks_markdown(folder_name, area_tasks))

        summary_md = render_owner_summary_markdown(self.index)
        atomic_write_text(self.config.vault_dir / "SUMMARY.md", summary_md)

    def run_pending_consolidation(self, *, client: ClientIdentity, ctx: dict[str, Any]) -> list[dict[str, Any]]:
        """Process pending consolidation jobs synchronously (one at a time)."""
        processed: list[dict[str, Any]] = []
        for job in self.index.list_jobs(status="pending"):
            if job["job_type"] != "consolidate_session":
                continue
            job_id = job["job_id"]
            ses_id = job["payload"]["session_id"]
            scope_id = job["payload"]["scope_id"]
            if not scope_permitted(scope_id, ctx["allowed_scopes"]):
                continue

            self.index.update_job_status(job_id, "running", inc_attempts=True)
            try:
                evt_rows = self.index.list_session_events(ses_id)
                events = [json.loads(Path(r["file_path"]).read_text(encoding="utf-8")) for r in evt_rows if Path(r["file_path"]).exists()]
                relevant = self.index.list_records(kind="memory", scopes=[scope_id], include_archived=False)[:20]
                result = self.provider.consolidate(
                    events=events,
                    relevant_memories=relevant,
                    schema_version="1.0",
                    policy_constraints={"scope_id": scope_id},
                )
                created: list[str] = []
                for idx, prop in enumerate(result.proposals):
                    idem_key = f"consolidate:{job_id}:{idx}"
                    if prop.get("kind") == "memory":
                        resp = self.dispatch(
                            client=client,
                            operation="create_memory",
                            context_id=ctx["context_id"],
                            idempotency_key=idem_key,
                            payload={
                                "type": prop["type"],
                                "scope_id": scope_id,
                                "title": prop["title"],
                                "body": prop["body"],
                                "origin": prop["origin"],
                                "status": prop.get("status", "current"),
                                "sources": prop["sources"],
                                "processing": result.provider_metadata,
                            },
                        )
                    elif prop.get("kind") == "task":
                        resp = self.dispatch(
                            client=client,
                            operation="create_task",
                            context_id=ctx["context_id"],
                            idempotency_key=idem_key,
                            payload={
                                "scope_id": scope_id,
                                "title": prop["title"],
                                "body": prop.get("body", prop["title"]),
                                "origin": prop.get("origin", "assistant_proposal"),
                                "proposal": True,
                                "status": "inbox",
                                "sources": prop["sources"],
                            },
                        )
                    else:
                        continue
                    if resp.get("ok"):
                        created.append(resp["data"]["record_id"])
                self.index.update_job_status(job_id, "completed")
                self.index.set_meta("last_consolidation_at", utc_now_iso())
                processed.append({"job_id": job_id, "created_ids": created})
            except Exception as exc:
                self.index.update_job_status(job_id, "failed", last_error=str(exc))
                processed.append({"job_id": job_id, "error": str(exc)})
        return processed

    def health(self) -> dict[str, Any]:
        usage = shutil.disk_usage(self.config.data_root)
        stats = self.index.stats()
        repair_ids = self.journal.list_repair_required()
        return {
            "status": "degraded" if repair_ids else "ok",
            "policy_version": self.config.policy_version,
            "timezone": self.config.timezone,
            "repair_required_mutations": repair_ids,
            "disk_free_bytes": usage.free,
            **stats,
        }

    def _validate_sources(self, sources: Any) -> None:
        if not isinstance(sources, list) or not sources:
            raise MemoryServiceError("validation_failed", "At least one source reference is required")
        for s in sources:
            if not isinstance(s, dict) or not s.get("record_id"):
                raise MemoryServiceError("validation_failed", "Each source must be an object with 'record_id'")
            if self.index.is_tombstoned(record_id=str(s["record_id"])):
                raise MemoryServiceError("validation_failed", "Cannot cite a forgotten/tombstoned source")

    _HANDLERS = {
        "start_session": start_session,
        "append_events": append_events,
        "end_session": end_session,
        "search_memory": search_memory,
        "read_page": read_page,
        "create_memory": create_memory,
        "update_memory": update_memory,
        "create_task": create_task,
        "update_task": update_task,
        "ingest_document": ingest_document,
        "archive_record": archive_record,
        "forget_record": forget_record,
        "list_areas": list_areas,
        "register_area": register_area,
    }
