"""Unified Application Core implementing all logical operations (Design Section 9)."""

from __future__ import annotations

import base64
import json
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .access import AccessPolicy, ClientIdentity, intersect_scopes, require_scope
from .bootstrap import init_memory_home
from .config import MemoryConfig
from .consolidation import ConsolidationProvider, StubConsolidationProvider
from .documents import delete_document_artifacts
from .errors import MemoryServiceError
from .frontmatter import content_hash, render_markdown
from .ids import canonical_json_bytes, canonical_request_hash, new_id, sha256_bytes, sha256_text, utc_now, utc_now_iso
from .index import MemoryIndex, scope_permitted
from .journal import FileMutationOp, MutationJournal
from .schemas import SchemaRegistry
from .scopes import AreaRegistry, normalize_scope_id, sanitize_subpath
from .storage import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_text,
    ensure_within,
    event_record_path,
    record_vault_path,
)
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

_WHITESPACE_RE = re.compile(r"\s+")


def _norm_ws(text: str) -> str:
    return _WHITESPACE_RE.sub(" ", text).strip()


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
        else:
            # Always reconcile vault on clean startup so manual Obsidian notes/edits are discovered (R12)
            reconciled = self.index.reconcile_vault(self.config.vault_dir)
            if reconciled["added"] or reconciled["updated"] or reconciled["removed"]:
                self.refresh_derived_views()

        self.index.recover_abandoned_jobs()

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

            if operation == "open_context":
                data = self.open_context(client=client, payload=inp)
                return {
                    "request_id": request_id,
                    "ok": True,
                    "data": data,
                    "policy_version": self.config.policy_version,
                    "warnings": warnings,
                }

            # Always validate context & current credential permissions FIRST (R01, R02)
            ctx = self._require_context(client, context_id)
            handler = self._HANDLERS.get(operation)
            if handler is None:
                raise MemoryServiceError("validation_failed", f"Unknown operation: {operation}")

            if operation not in MUTATING_OPERATIONS:
                data = handler(self, client=client, ctx=ctx, payload=inp, warnings=warnings)
                return {
                    "request_id": request_id,
                    "ok": True,
                    "data": data,
                    "policy_version": self.config.policy_version,
                    "warnings": warnings,
                }

            # Mutating operation: check write permission and mandatory idempotency_key
            if not client.can_write:
                raise MemoryServiceError("authentication_required", "Client credential is read-only")
            if not idempotency_key or not isinstance(idempotency_key, str) or not idempotency_key.strip():
                raise MemoryServiceError(
                    "validation_failed",
                    f"idempotency_key is mandatory for operation '{operation}'",
                )

            req_hash = canonical_request_hash(operation, inp)

            # Fast-path unlocked check (allows concurrent callers to hit idempotency check cleanly)
            cached = self._check_idempotency_receipt(client.client_id, idempotency_key, operation, req_hash)
            if cached is not None:
                return cached

            # Coordinated mutation boundary prevents concurrent duplicates (R08) and recovers
            # committed journal entries if SQLite receipt persistence previously failed (R09)
            with self.journal.mutation_boundary():
                cached_locked = self._check_idempotency_locked(client.client_id, idempotency_key, operation, req_hash)
                if cached_locked is not None:
                    return cached_locked

                data = handler(
                    self,
                    client=client,
                    ctx=ctx,
                    payload=inp,
                    warnings=warnings,
                    idempotency_meta={
                        "client_id": client.client_id,
                        "idempotency_key": idempotency_key,
                        "request_hash": req_hash,
                    },
                )
                response = {
                    "request_id": request_id,
                    "ok": True,
                    "data": data,
                    "policy_version": self.config.policy_version,
                    "warnings": warnings,
                }
                mut_id = data.get("mutation_id")
                if isinstance(mut_id, str):
                    self.journal.record_response(mut_id, response)
                self.index.save_idempotency(
                    client_id=client.client_id,
                    key=idempotency_key,
                    operation=operation,
                    request_hash=req_hash,
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

    def _check_idempotency_receipt(
        self, client_id: str, key: str, operation: str, req_hash: str
    ) -> dict[str, Any] | None:
        existing = self.index.get_idempotency(client_id, key)
        if existing is not None:
            if existing["request_hash"] != req_hash or existing["operation"] != operation:
                raise MemoryServiceError(
                    "idempotency_conflict",
                    "Idempotency key already used with a different operation or payload",
                )
            return json.loads(existing["response_json"])
        return None

    def _check_idempotency_locked(
        self, client_id: str, key: str, operation: str, req_hash: str
    ) -> dict[str, Any] | None:
        # 1. Direct SQLite check (bypasses any test hook on get_idempotency to prevent barrier deadlock)
        with self.index._lock:
            row = self.index._conn.execute(
                "SELECT * FROM idempotency_receipts WHERE client_id = ? AND idempotency_key = ?",
                (client_id, key),
            ).fetchone()
        if row is not None:
            if row["request_hash"] != req_hash or row["operation"] != operation:
                raise MemoryServiceError(
                    "idempotency_conflict",
                    "Idempotency key already used with a different operation or payload",
                )
            return json.loads(row["response_json"])

        # 2. Durable journal fallback (R09: recovers if previous run crashed before save_idempotency)
        j_entry = self.journal.find_by_idempotency(client_id, key)
        if j_entry is not None:
            if j_entry.get("request_hash") != req_hash or j_entry.get("operation") != operation:
                raise MemoryServiceError(
                    "idempotency_conflict",
                    "Idempotency key already committed with a different operation or payload",
                )
            recovered_resp = j_entry.get("response")
            if isinstance(recovered_resp, dict):
                self.index.save_idempotency(
                    client_id=client_id,
                    key=key,
                    operation=operation,
                    request_hash=req_hash,
                    response=recovered_resp,
                )
                return recovered_resp
        return None

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
        # Always intersect saved context scopes with the credential's CURRENT scopes (R01)
        effective_scopes = intersect_scopes(client.allowed_scopes, ctx["allowed_scopes"])
        if not effective_scopes:
            raise MemoryServiceError(
                "authentication_required",
                "Current credential no longer permits any scopes in this context",
            )
        active = dict(ctx)
        active["allowed_scopes"] = effective_scopes
        return active

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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        app_label = str(payload.get("source_application") or "").strip()
        coverage = str(payload.get("capture_coverage") or "").strip()
        scope_id = normalize_scope_id(str(payload.get("scope_id") or ctx["allowed_scopes"][0]))
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
        idem = idempotency_meta or {}
        entry = self.journal.execute(
            operation="start_session",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"session_id": ses_id},
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

            if source_event_id and self.index.is_tombstoned(fingerprint=f"src:{client.client_id}:{source_event_id}"):
                skipped_tombstoned.append(str(source_event_id))
                warnings.append(f"Skipped previously forgotten source_event_id '{source_event_id}'")
                continue

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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

        target_path = Path(ses_rec["file_path"])
        self._save_prior_revision(ses_rec, target_path, actor=client.client_id, reason="Session ended")

        meta = dict(ses_rec["metadata"])
        meta["ended_at"] = utc_now_iso()
        meta["revision"] = int(meta["revision"]) + 1
        self.schemas.validate("session", meta)

        rendered = render_markdown(meta, ses_rec["body"])
        idem = idempotency_meta or {}
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
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
            "mutation_id": entry["mutation_id"],
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
        effective_scopes = (
            intersect_scopes(ctx["allowed_scopes"], filter_scopes) if filter_scopes else ctx["allowed_scopes"]
        )
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
                "sources": self._filter_sources_for_scope(r["metadata"].get("sources", []), ctx["allowed_scopes"]),
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

        # 1. Allowlisted read-only policy documents (R17)
        policy_resp = self._try_read_policy_document(record_id, max_chars=max_chars)
        if policy_resp is not None:
            return policy_resp

        # 2. Authorized raw event read for citation verification (R17)
        if record_id.startswith("evt_"):
            evt = self.index.get_event(record_id)
            if evt is None:
                raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
            require_scope(evt["scope_id"], ctx["allowed_scopes"], hide_existence=True)
            evt_path = Path(evt["file_path"])
            evt_doc = json.loads(evt_path.read_text(encoding="utf-8")) if evt_path.is_file() else evt
            content = str(evt_doc.get("content") or "")
            return {
                "id": record_id,
                "kind": "event",
                "scope_id": evt["scope_id"],
                "revision": 1,
                "metadata": {k: v for k, v in evt_doc.items() if k != "content"},
                "body": content[:max_chars],
                "truncated": len(content) > max_chars,
                "related": [],
            }

        # 3. Reconcile record file on disk before returning so manual Obsidian edits are visible (R12)
        rec = self.index.reconcile_record_file(record_id)
        if rec is None:
            raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
        require_scope(rec["scope_id"], ctx["allowed_scopes"], hide_existence=True)

        # Filter related records AND sanitize metadata strictly by scope so private IDs never leak (R04)
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

        sanitized_meta = self._sanitize_metadata(rec["metadata"], ctx["allowed_scopes"])
        body = rec["body"][:max_chars]
        return {
            "id": rec["id"],
            "kind": rec["kind"],
            "scope_id": rec["scope_id"],
            "revision": rec["revision"],
            "metadata": sanitized_meta,
            "body": body,
            "truncated": len(rec["body"]) > max_chars,
            "related": permitted_rels,
        }

    def _try_read_policy_document(self, record_id: str, *, max_chars: int) -> dict[str, Any] | None:
        """Allow reading versioned policy files advertised in bootstrap (R17)."""
        norm = record_id.replace("\\", "/").lstrip("/")
        if norm != "START-HERE.md" and not norm.startswith("System/Rules/") and not norm.startswith("System/Templates/"):
            return None
        if not norm.endswith(".md"):
            return None
        candidate = ensure_within(self.config.vault_dir, self.config.vault_dir / norm)
        if not candidate.is_file():
            return None
        raw = candidate.read_text(encoding="utf-8")
        return {
            "id": norm,
            "kind": "policy",
            "scope_id": "shared",
            "revision": 1,
            "metadata": {"path": norm, "policy_version": self.config.policy_version, "read_only": True},
            "body": raw[:max_chars],
            "truncated": len(raw) > max_chars,
            "related": [],
        }

    def _sanitize_metadata(self, meta: dict[str, Any], allowed_scopes: list[str]) -> dict[str, Any]:
        """Strip any restricted relation, source, or dependency IDs from returned metadata (R04)."""
        clean = dict(meta)
        if "relations" in clean and isinstance(clean["relations"], list):
            filtered_rels = []
            for rel in clean["relations"]:
                if not isinstance(rel, dict) or not rel.get("target_id"):
                    continue
                target = self.index.get_record(str(rel["target_id"]))
                if target is not None and scope_permitted(target["scope_id"], allowed_scopes):
                    filtered_rels.append(rel)
            clean["relations"] = filtered_rels

        if "sources" in clean and isinstance(clean["sources"], list):
            clean["sources"] = self._filter_sources_for_scope(clean["sources"], allowed_scopes)

        if "depends_on" in clean and isinstance(clean["depends_on"], list):
            filtered_deps = []
            for dep_id in clean["depends_on"]:
                dep = self.index.get_record(str(dep_id))
                if dep is not None and scope_permitted(dep["scope_id"], allowed_scopes):
                    filtered_deps.append(dep_id)
            clean["depends_on"] = filtered_deps

        return clean

    def _filter_sources_for_scope(self, sources: list[Any], allowed_scopes: list[str]) -> list[Any]:
        out: list[Any] = []
        for s in sources:
            if not isinstance(s, dict) or not s.get("record_id"):
                continue
            sid = str(s["record_id"])
            src_scope = self._resolve_source_scope(sid)
            if src_scope is not None and scope_permitted(src_scope, allowed_scopes):
                out.append(s)
        return out

    # -- Memories -------------------------------------------------------------

    def create_memory(
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope_id = normalize_scope_id(str(payload.get("scope_id") or ""))
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
        self._validate_sources(sources, caller_scopes=ctx["allowed_scopes"], target_scope=scope_id)

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
        idem = idempotency_meta or {}
        entry = self.journal.execute(
            operation="create_memory",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"record_id": mem_id},
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

        # Enforce epistemic inference discipline on updates too (R07)
        if meta.get("origin") == "inference" and changes.get("status") == "current":
            raise MemoryServiceError(
                "validation_failed",
                "An inference cannot be promoted to 'current' via status update; create a sourced explicit record or supersede it",
            )

        body = rec["body"]
        for key, val in changes.items():
            if key == "body":
                body = str(val)
            else:
                meta[key] = val

        if "sources" in changes:
            self._validate_sources(meta["sources"], caller_scopes=ctx["allowed_scopes"], target_scope=rec["scope_id"])

        if source_ids:
            new_source_objs = [{"record_id": str(sid)} for sid in source_ids]
            self._validate_sources(new_source_objs, caller_scopes=ctx["allowed_scopes"], target_scope=rec["scope_id"])
            existing_sources = list(meta.get("sources") or [])
            seen_ids = {s.get("record_id") for s in existing_sources}
            for obj in new_source_objs:
                if obj["record_id"] not in seen_ids:
                    existing_sources.append(obj)
                    seen_ids.add(obj["record_id"])
            meta["sources"] = existing_sources

        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = utc_now_iso()
        self.schemas.validate("memory", meta)

        target_path = Path(rec["file_path"])
        # Save durable historical revision before overwriting (R11)
        self._save_prior_revision(rec, target_path, actor=client.client_id, reason=reason)

        rendered = render_markdown(meta, body)
        idem = idempotency_meta or {}
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
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope_id = normalize_scope_id(str(payload.get("scope_id") or ""))
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
        self._validate_sources(sources, caller_scopes=ctx["allowed_scopes"], target_scope=scope_id)

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
        idem = idempotency_meta or {}
        entry = self.journal.execute(
            operation="create_task",
            ops=[FileMutationOp(path=target_path, action="write", after_content=rendered)],
            metadata={"record_id": task_id},
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

        if "sources" in changes:
            self._validate_sources(meta["sources"], caller_scopes=ctx["allowed_scopes"], target_scope=rec["scope_id"])

        new_status = meta["status"]
        new_deps = list(meta.get("depends_on") or [])

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
            new_source_objs = [{"record_id": str(sid)} for sid in source_ids]
            self._validate_sources(new_source_objs, caller_scopes=ctx["allowed_scopes"], target_scope=rec["scope_id"])
            existing_sources = list(meta.get("sources") or [])
            seen = {s.get("record_id") for s in existing_sources}
            for obj in new_source_objs:
                if obj["record_id"] not in seen:
                    existing_sources.append(obj)
                    seen.add(obj["record_id"])
            meta["sources"] = existing_sources

        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = now
        self.schemas.validate("task", meta)

        target_path = Path(rec["file_path"])
        self._save_prior_revision(rec, target_path, actor=client.client_id, reason=str(reason or "Task updated"))

        rendered = render_markdown(meta, body)
        idem = idempotency_meta or {}
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
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        scope_id = normalize_scope_id(str(payload.get("scope_id") or ""))
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
        idem = idempotency_meta or {}
        entry = self.journal.execute(
            operation="ingest_document",
            ops=[FileMutationOp(path=companion_path, action="write", after_content=rendered)],
            metadata={"record_id": doc_id},
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

        target_path = Path(rec["file_path"])
        self._save_prior_revision(rec, target_path, actor=client.client_id, reason=f"Archived: {reason}")

        meta = dict(rec["metadata"])
        meta["archived"] = True
        meta["archived_at"] = utc_now_iso()
        meta["archived_reason"] = reason
        meta["revision"] = rec["revision"] + 1
        meta["updated_at"] = utc_now_iso()

        rendered = render_markdown(meta, rec["body"])
        idem = idempotency_meta or {}
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
            client_id=idem.get("client_id"),
            idempotency_key=idem.get("idempotency_key"),
            request_hash=idem.get("request_hash"),
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
        return {
            "record_id": record_id,
            "archived": True,
            "revision": meta["revision"],
            "mutation_id": entry["mutation_id"],
        }

    def forget_record(
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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

        idem = idempotency_meta or {}
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
                client_id=idem.get("client_id"),
                idempotency_key=idem.get("idempotency_key"),
                request_hash=idem.get("request_hash"),
            )
            self.journal.mark_projecting(entry["mutation_id"])

            # Delete document original binary & extraction cache if applicable (R14)
            artifacts_removed: dict[str, bool] = {}
            if rec["kind"] == "document":
                artifacts_removed = delete_document_artifacts(
                    self.config,
                    doc_id=record_id,
                    original_attachment_path=rec["metadata"].get("original_attachment_path"),
                )

            self.index.delete_jobs_for_target(record_id)
            self.journal.delete_revision_history(record_id)
            self.index.delete_record(record_id)
            self.index.add_tombstone(
                record_id=record_id,
                kind=rec["kind"],
                scope_id=rec["scope_id"],
                fingerprints=[f"id:{record_id}"],
                reason=reason,
            )

            # Cascade cleanup to derived active records citing this record (R13)
            derived_forgotten = self._cascade_forget_derived(record_id, reason=f"Source {record_id} forgotten: {reason}")
            self.refresh_derived_views()
            self.journal.mark_completed(entry["mutation_id"])
            return {
                "record_id": record_id,
                "forgotten": True,
                "tombstone_recorded": True,
                "derived_forgotten_ids": derived_forgotten,
                "artifacts_removed": artifacts_removed,
                "mutation_id": entry["mutation_id"],
                "derived_cleanup": "active indexes, FTS5, revision history, derived records, and views cleaned",
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
            # Cascade cleanup to derived active records citing this event (R13)
            derived_forgotten = self._cascade_forget_derived(record_id, reason=f"Source {record_id} forgotten: {reason}")
            self.refresh_derived_views()
            return {
                "record_id": record_id,
                "forgotten": True,
                "tombstone_recorded": True,
                "derived_forgotten_ids": derived_forgotten,
                "derived_cleanup": "event file, index entry, and derived records without independent sources removed",
                "limitations": (
                    "Offline backups and any external provider logs previously sent are not retroactively "
                    "erased by this local deletion."
                ),
            }

        raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")

    def _cascade_forget_derived(self, forgotten_source_id: str, *, reason: str) -> list[str]:
        """Recursively remove derived records whose only evidence was forgotten, or strip the forgotten citation (R13)."""
        forgotten_ids: list[str] = []
        queue = [forgotten_source_id]
        visited: set[str] = {forgotten_source_id}

        while queue:
            src_id = queue.pop(0)
            dependents = self.index.list_records_citing(src_id)
            for dep in dependents:
                dep_id = dep["id"]
                if dep_id in visited:
                    continue
                meta = dict(dep["metadata"])
                remaining_sources = [
                    s for s in (meta.get("sources") or []) if isinstance(s, dict) and s.get("record_id") != src_id
                ]
                dep_path = Path(dep["file_path"])
                if not remaining_sources:
                    # No independent surviving evidence -> remove derived active copy completely
                    if dep_path.is_file():
                        dep_path.unlink()
                    self.journal.delete_revision_history(dep_id)
                    self.index.delete_jobs_for_target(dep_id)
                    self.index.delete_record(dep_id)
                    self.index.add_tombstone(
                        record_id=dep_id,
                        kind=dep["kind"],
                        scope_id=dep["scope_id"],
                        fingerprints=[f"id:{dep_id}"],
                        reason=reason,
                    )
                    forgotten_ids.append(dep_id)
                    visited.add(dep_id)
                    queue.append(dep_id)
                else:
                    # Independent evidence survives -> strip the forgotten source citation and refresh
                    meta["sources"] = remaining_sources
                    meta["revision"] = int(meta.get("revision", 1)) + 1
                    meta["updated_at"] = utc_now_iso()
                    rendered = render_markdown(meta, dep["body"])
                    atomic_write_text(dep_path, rendered)
                    self.index.upsert_record(
                        record_id=dep_id,
                        kind=dep["kind"],
                        meta=meta,
                        body=dep["body"],
                        file_path=dep_path,
                        raw_hash=content_hash(rendered),
                    )
        return forgotten_ids

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
        self,
        *,
        client: ClientIdentity,
        ctx: dict[str, Any],
        payload: dict[str, Any],
        warnings: list[str],
        idempotency_meta: dict[str, Any] | None = None,
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
            if not scope_permitted(job["payload"]["scope_id"], ctx["allowed_scopes"]):
                continue
            processed.append(self._run_single_consolidation_job(job=job, client=client, ctx=ctx))
        return processed

    def _run_single_consolidation_job(
        self, *, job: dict[str, Any], client: ClientIdentity, ctx: dict[str, Any]
    ) -> dict[str, Any]:
        """Process one consolidation job with explicit per-proposal outcome accounting (R20)."""
        job_id = job["job_id"]
        ses_id = job["payload"]["session_id"]
        scope_id = job["payload"]["scope_id"]
        lease = (utc_now() + timedelta(seconds=120)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.index.update_job_status(job_id, "running", inc_attempts=True, lease_until=lease)

        try:
            evt_rows = self.index.list_session_events(ses_id)
            events = [
                json.loads(Path(r["file_path"]).read_text(encoding="utf-8"))
                for r in evt_rows
                if Path(r["file_path"]).is_file()
            ]
            relevant = self.index.list_records(kind="memory", scopes=[scope_id], include_archived=False)[:20]
            result = self.provider.consolidate(
                events=events,
                relevant_memories=relevant,
                schema_version="1.0",
                policy_constraints={"scope_id": scope_id},
            )
            created: list[str] = []
            rejected: list[dict[str, Any]] = []
            for prop in result.proposals:
                # Deterministic replay key independent of list ordering (R20)
                prop_hash = sha256_bytes(canonical_json_bytes(prop))[:16]
                idem_key = f"consolidate:{job_id}:{prop_hash}"
                kind = prop.get("kind")
                if kind == "memory":
                    resp = self.dispatch(
                        client=client,
                        operation="create_memory",
                        context_id=ctx["context_id"],
                        idempotency_key=idem_key,
                        payload={
                            "type": prop.get("type"),
                            "scope_id": scope_id,
                            "title": prop.get("title"),
                            "body": prop.get("body"),
                            "origin": prop.get("origin"),
                            "status": prop.get("status", "current"),
                            "sources": prop.get("sources"),
                            "processing": result.provider_metadata,
                        },
                    )
                elif kind == "task":
                    resp = self.dispatch(
                        client=client,
                        operation="create_task",
                        context_id=ctx["context_id"],
                        idempotency_key=idem_key,
                        payload={
                            "scope_id": scope_id,
                            "title": prop.get("title"),
                            "body": prop.get("body") or prop.get("title"),
                            "origin": prop.get("origin", "assistant_proposal"),
                            "proposal": True,
                            "status": "inbox",
                            "sources": prop.get("sources"),
                        },
                    )
                else:
                    rejected.append({"proposal_hash": prop_hash, "error": f"Unsupported proposal kind: {kind}"})
                    continue

                if resp.get("ok"):
                    created.append(resp["data"]["record_id"])
                else:
                    rejected.append({"proposal_hash": prop_hash, "error": resp.get("error")})

            if rejected:
                err_msg = f"{len(rejected)} proposal(s) rejected during consolidation: {json.dumps(rejected)}"
                self.index.update_job_status(job_id, "failed", last_error=err_msg)
                return {"job_id": job_id, "status": "failed", "created_ids": created, "rejected": rejected}

            self.index.update_job_status(job_id, "completed")
            self.index.set_meta("last_consolidation_at", utc_now_iso())
            return {"job_id": job_id, "status": "completed", "created_ids": created, "rejected": []}
        except Exception as exc:
            next_state = "failed" if job["attempts"] + 1 >= job["max_attempts"] else "retry_wait"
            self.index.update_job_status(job_id, next_state, last_error=str(exc))
            return {"job_id": job_id, "status": next_state, "error": str(exc)}

    def health(self) -> dict[str, Any]:
        usage = shutil.disk_usage(self.config.data_root)
        stats = self.index.stats()
        repair_ids = self.journal.list_repair_required()
        return {
            "status": "degraded" if repair_ids else "ok",
            "policy_version": self.config.policy_version,
            "timezone": self.config.timezone,
            "repair_required_mutations": repair_ids,
            "last_reconcile_at": self.index.get_meta("last_reconcile_at"),
            "disk_free_bytes": usage.free,
            **stats,
        }

    # -- Provenance & Revision Helpers ----------------------------------------

    def _save_prior_revision(self, rec: dict[str, Any], target_path: Path, *, actor: str, reason: str) -> None:
        raw_md = target_path.read_text(encoding="utf-8") if target_path.is_file() else ""
        self.journal.save_revision_snapshot(
            record_id=rec["id"],
            revision=rec["revision"],
            actor=actor,
            reason=reason,
            metadata=rec["metadata"],
            body=rec["body"],
            raw_markdown=raw_md,
        )

    def _resolve_source_scope(self, source_id: str) -> str | None:
        evt = self.index.get_event(source_id)
        if evt is not None:
            return evt["scope_id"]
        rec = self.index.get_record(source_id)
        if rec is not None:
            return rec["scope_id"]
        return None

    def _validate_sources(self, sources: Any, *, caller_scopes: list[str], target_scope: str) -> None:
        """
        Validate every cited source exists, is not forgotten, is readable by the caller,
        does not broaden disclosure across scopes (R05, R06), and matches any cited quote.
        """
        if not isinstance(sources, list) or not sources:
            raise MemoryServiceError("validation_failed", "At least one source reference is required")

        norm_target = normalize_scope_id(target_scope)
        for s in sources:
            if not isinstance(s, dict) or not s.get("record_id"):
                raise MemoryServiceError("validation_failed", "Each source must be an object with 'record_id'")
            sid = str(s["record_id"])
            if self.index.is_tombstoned(record_id=sid):
                raise MemoryServiceError("validation_failed", "Cannot cite a forgotten/tombstoned source")

            src_scope: str | None = None
            src_text: str = ""
            evt = self.index.get_event(sid)
            if evt is not None:
                src_scope = evt["scope_id"]
                evt_path = Path(evt["file_path"])
                if evt_path.is_file():
                    src_text = str(json.loads(evt_path.read_text(encoding="utf-8")).get("content") or "")
            else:
                rec = self.index.get_record(sid)
                if rec is not None:
                    src_scope = rec["scope_id"]
                    src_text = str(rec.get("body") or "")

            if src_scope is None:
                raise MemoryServiceError("validation_failed", f"Referenced source '{sid}' does not exist")

            # Caller must be authorized to access the cited source (R06)
            if not scope_permitted(src_scope, caller_scopes):
                raise MemoryServiceError("not_found_or_unavailable", "Referenced source not found or unavailable")

            # Disclosure inheritance: destination cannot be broader/different than source (unless source is 'shared')
            norm_src = normalize_scope_id(src_scope)
            if norm_src != "shared" and not scope_permitted(norm_target, [norm_src]):
                raise MemoryServiceError(
                    "validation_failed",
                    f"Cannot cite source from scope '{norm_src}' into '{norm_target}' without explicit sharing",
                )

            # Verify quoted passage against source content when provided (R05)
            quote = s.get("quote")
            if isinstance(quote, str) and quote.strip():
                if _norm_ws(quote) not in _norm_ws(src_text):
                    raise MemoryServiceError(
                        "validation_failed",
                        f"Cited quote was not found in source '{sid}'",
                    )

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
