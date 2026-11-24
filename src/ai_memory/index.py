"""Rebuildable SQLite WAL + FTS5 search, relationship, and operational index."""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .frontmatter import content_hash, parse_markdown
from .ids import utc_now_iso

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS records (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    record_type TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    project_id TEXT,
    title TEXT NOT NULL,
    status TEXT NOT NULL,
    origin TEXT,
    revision INTEGER NOT NULL,
    file_path TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    body_text TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_records_scope ON records(scope_id);
CREATE INDEX IF NOT EXISTS idx_records_kind_status ON records(kind, status, archived);

CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
    id UNINDEXED,
    title,
    body_text,
    scope_id UNINDEXED
);

CREATE TABLE IF NOT EXISTS relations (
    source_id TEXT NOT NULL,
    rel_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    PRIMARY KEY (source_id, rel_type, target_id)
);

CREATE INDEX IF NOT EXISTS idx_relations_target ON relations(target_id);

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    client_id TEXT NOT NULL,
    source_event_id TEXT,
    sequence INTEGER NOT NULL,
    speaker TEXT NOT NULL,
    event_type TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    file_path TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_events_dedup
    ON events(client_id, source_event_id)
    WHERE source_event_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS idempotency_receipts (
    client_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    operation TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (client_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS contexts (
    context_id TEXT PRIMARY KEY,
    client_id TEXT NOT NULL,
    allowed_scopes_json TEXT NOT NULL,
    purpose TEXT NOT NULL,
    project_id TEXT,
    policy_version TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tombstones (
    record_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    scope_id TEXT NOT NULL,
    fingerprints_json TEXT NOT NULL,
    forgotten_at TEXT NOT NULL,
    reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    lease_until TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class MemoryIndex:
    """Thread-safe SQLite WAL + FTS5 index."""

    def __init__(self, config: MemoryConfig) -> None:
        self._config = config
        self._config.runtime_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(config.db_path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA_SQL)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- Meta helpers ---------------------------------------------------------

    def set_meta(self, key: str, value: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            self._conn.commit()

    def get_meta(self, key: str) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    # -- Record indexing ------------------------------------------------------

    def upsert_record(
        self,
        *,
        record_id: str,
        kind: str,
        meta: dict[str, Any],
        body: str,
        file_path: Path,
        raw_hash: str,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO records(
                    id, kind, record_type, scope_id, project_id, title, status, origin,
                    revision, file_path, content_hash, archived, recorded_at, updated_at,
                    metadata_json, body_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    kind=excluded.kind,
                    record_type=excluded.record_type,
                    scope_id=excluded.scope_id,
                    project_id=excluded.project_id,
                    title=excluded.title,
                    status=excluded.status,
                    origin=excluded.origin,
                    revision=excluded.revision,
                    file_path=excluded.file_path,
                    content_hash=excluded.content_hash,
                    archived=excluded.archived,
                    recorded_at=excluded.recorded_at,
                    updated_at=excluded.updated_at,
                    metadata_json=excluded.metadata_json,
                    body_text=excluded.body_text
                """,
                (
                    record_id,
                    kind,
                    str(meta.get("type", kind)),
                    str(meta["scope_id"]),
                    meta.get("project_id"),
                    str(meta.get("title", record_id)),
                    str(meta.get("status", "current")),
                    meta.get("origin"),
                    int(meta.get("revision", 1)),
                    str(file_path),
                    raw_hash,
                    1 if meta.get("archived") else 0,
                    str(meta.get("recorded_at") or meta.get("started_at") or meta.get("acquired_at") or utc_now_iso()),
                    str(meta.get("updated_at") or meta.get("started_at") or meta.get("acquired_at") or utc_now_iso()),
                    json.dumps(meta, ensure_ascii=False),
                    body,
                ),
            )
            self._conn.execute("DELETE FROM records_fts WHERE id = ?", (record_id,))
            self._conn.execute(
                "INSERT INTO records_fts(id, title, body_text, scope_id) VALUES (?, ?, ?, ?)",
                (record_id, str(meta.get("title", "")), body, str(meta["scope_id"])),
            )
            self._conn.execute("DELETE FROM relations WHERE source_id = ?", (record_id,))
            for rel in meta.get("relations", []) or []:
                if isinstance(rel, dict) and rel.get("type") and rel.get("target_id"):
                    self._conn.execute(
                        "INSERT OR IGNORE INTO relations(source_id, rel_type, target_id) VALUES (?, ?, ?)",
                        (record_id, rel["type"], rel["target_id"]),
                    )
            self.set_meta("index_updated_at", utc_now_iso())
            self._conn.commit()

    def delete_record(self, record_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM records WHERE id = ?", (record_id,))
            self._conn.execute("DELETE FROM records_fts WHERE id = ?", (record_id,))
            self._conn.execute("DELETE FROM relations WHERE source_id = ? OR target_id = ?", (record_id, record_id))
            self._conn.commit()

    def get_record(self, record_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM records WHERE id = ?", (record_id,)).fetchone()
            if not row:
                return None
            return self._row_to_record(row)

    def list_records(
        self,
        *,
        kind: str | None = None,
        scopes: list[str] | None = None,
        include_archived: bool = False,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if not include_archived:
            clauses.append("archived = 0")
        if status:
            clauses.append("status = ?")
            params.append(status)

        sql = "SELECT * FROM records"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC"

        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()

        results = [self._row_to_record(r) for r in rows]
        if scopes is not None:
            results = [r for r in results if scope_permitted(r["scope_id"], scopes)]
        return results

    def search_records(
        self,
        *,
        query: str,
        permitted_scopes: list[str],
        kind: str | None = None,
        include_archived: bool = False,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Scoped FTS5 search: scope restriction is enforced before returning any row."""
        clean_q = query.strip()
        with self._lock:
            if clean_q:
                # Escape double quotes and run prefix/token FTS5 match safely
                tokens = [t.replace('"', "") for t in clean_q.split() if t.replace('"', "")]
                fts_expr = " AND ".join(f'"{t}"*' for t in tokens) if tokens else '""'
                try:
                    rows = self._conn.execute(
                        """
                        SELECT r.*, snippet(records_fts, 2, '[', ']', '...', 24) AS excerpt
                        FROM records_fts
                        JOIN records r ON r.id = records_fts.id
                        WHERE records_fts MATCH ?
                        ORDER BY bm25(records_fts)
                        LIMIT ?
                        """,
                        (fts_expr, limit * 4),
                    ).fetchall()
                except sqlite3.OperationalError:
                    rows = []
            else:
                rows = self._conn.execute(
                    "SELECT *, substr(body_text, 1, 200) AS excerpt FROM records ORDER BY updated_at DESC LIMIT ?",
                    (limit * 4,),
                ).fetchall()

        out: list[dict[str, Any]] = []
        for row in rows:
            rec = self._row_to_record(row)
            if not scope_permitted(rec["scope_id"], permitted_scopes):
                continue
            if kind and rec["kind"] != kind:
                continue
            if not include_archived and rec["archived"]:
                continue
            rec["excerpt"] = row["excerpt"]
            out.append(rec)
            if len(out) >= limit:
                break
        return out

    def get_relations(self, record_id: str) -> list[dict[str, str]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT source_id, rel_type, target_id FROM relations WHERE source_id = ? OR target_id = ?",
                (record_id, record_id),
            ).fetchall()
            return [dict(r) for r in rows]

    def rebuild_from_vault(self, vault_dir: Path) -> int:
        """Rebuild records, FTS5, and relations tables solely from canonical Markdown files."""
        with self._lock:
            self._conn.execute("DELETE FROM records")
            self._conn.execute("DELETE FROM records_fts")
            self._conn.execute("DELETE FROM relations")
            self._conn.commit()

        count = 0
        for md_path in sorted(vault_dir.rglob("*.md")):
            rel = md_path.relative_to(vault_dir)
            if rel.parts and rel.parts[0] == "System":
                continue
            raw = md_path.read_text(encoding="utf-8")
            try:
                meta, body = parse_markdown(raw)
            except Exception:
                continue
            record_id = meta.get("id")
            if not isinstance(record_id, str) or not meta.get("scope_id"):
                continue
            kind = _infer_kind(record_id, str(meta.get("type", "")))
            if kind is None:
                continue
            self.upsert_record(
                record_id=record_id,
                kind=kind,
                meta=meta,
                body=body,
                file_path=md_path,
                raw_hash=content_hash(raw),
            )
            count += 1
        self.set_meta("last_rebuild_at", utc_now_iso())
        return count

    # -- Events ---------------------------------------------------------------

    def find_event_by_source(self, client_id: str, source_event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM events WHERE client_id = ? AND source_event_id = ?",
                (client_id, source_event_id),
            ).fetchone()
            return dict(row) if row else None

    def get_event(self, event_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM events WHERE id = ?", (event_id,)).fetchone()
            return dict(row) if row else None

    def insert_event(self, evt: dict[str, Any], *, client_id: str, c_hash: str, file_path: Path) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO events(
                    id, session_id, scope_id, client_id, source_event_id,
                    sequence, speaker, event_type, recorded_at, content_hash, file_path
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    evt["id"],
                    evt["session_id"],
                    evt["scope_id"],
                    client_id,
                    evt.get("source_event_id"),
                    int(evt["sequence"]),
                    evt["speaker"],
                    evt["event_type"],
                    evt["recorded_at"],
                    c_hash,
                    str(file_path),
                ),
            )
            self.set_meta("last_capture_at", evt["recorded_at"])
            self._conn.commit()

    def list_session_events(self, session_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM events WHERE session_id = ? ORDER BY sequence ASC",
                (session_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def delete_event_index(self, event_id: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM events WHERE id = ?", (event_id,))
            self._conn.commit()

    # -- Idempotency ----------------------------------------------------------

    def get_idempotency(self, client_id: str, key: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM idempotency_receipts WHERE client_id = ? AND idempotency_key = ?",
                (client_id, key),
            ).fetchone()
            return dict(row) if row else None

    def save_idempotency(
        self, *, client_id: str, key: str, operation: str, request_hash: str, response: dict[str, Any]
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO idempotency_receipts(
                    client_id, idempotency_key, operation, request_hash, response_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (client_id, key, operation, request_hash, json.dumps(response, ensure_ascii=False), utc_now_iso()),
            )
            self._conn.commit()

    # -- Contexts -------------------------------------------------------------

    def save_context(self, ctx: dict[str, Any]) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO contexts(
                    context_id, client_id, allowed_scopes_json, purpose, project_id,
                    policy_version, created_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ctx["context_id"],
                    ctx["client_id"],
                    json.dumps(ctx["allowed_scopes"]),
                    ctx["purpose"],
                    ctx.get("project_id"),
                    ctx["policy_version"],
                    ctx["created_at"],
                    ctx["expires_at"],
                ),
            )
            self._conn.commit()

    def get_context(self, context_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("SELECT * FROM contexts WHERE context_id = ?", (context_id,)).fetchone()
            if not row:
                return None
            out = dict(row)
            out["allowed_scopes"] = json.loads(out.pop("allowed_scopes_json"))
            return out

    # -- Tombstones -----------------------------------------------------------

    def add_tombstone(
        self, *, record_id: str, kind: str, scope_id: str, fingerprints: list[str], reason: str
    ) -> None:
        with self._lock:
            self._conn.execute(
                """
                INSERT OR REPLACE INTO tombstones(
                    record_id, kind, scope_id, fingerprints_json, forgotten_at, reason
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (record_id, kind, scope_id, json.dumps(fingerprints), utc_now_iso(), reason),
            )
            self._conn.commit()

    def is_tombstoned(self, *, record_id: str | None = None, fingerprint: str | None = None) -> bool:
        with self._lock:
            if record_id:
                row = self._conn.execute("SELECT 1 FROM tombstones WHERE record_id = ?", (record_id,)).fetchone()
                if row:
                    return True
            if fingerprint:
                rows = self._conn.execute("SELECT fingerprints_json FROM tombstones").fetchall()
                for row in rows:
                    if fingerprint in json.loads(row["fingerprints_json"]):
                        return True
            return False

    # -- Jobs -----------------------------------------------------------------

    def enqueue_job(self, *, job_id: str, job_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        now = utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO jobs(job_id, job_type, status, payload_json, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?, ?)
                """,
                (job_id, job_type, json.dumps(payload, ensure_ascii=False), now, now),
            )
            self._conn.commit()
        return {"job_id": job_id, "job_type": job_type, "status": "pending", "created_at": now}

    def list_jobs(self, *, status: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            if status:
                rows = self._conn.execute("SELECT * FROM jobs WHERE status = ? ORDER BY created_at ASC", (status,)).fetchall()
            else:
                rows = self._conn.execute("SELECT * FROM jobs ORDER BY created_at ASC").fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["payload"] = json.loads(item.pop("payload_json"))
            out.append(item)
        return out

    def update_job_status(self, job_id: str, status: str, *, last_error: str | None = None, inc_attempts: bool = False) -> None:
        with self._lock:
            if inc_attempts:
                self._conn.execute(
                    "UPDATE jobs SET status=?, attempts=attempts+1, last_error=?, updated_at=? WHERE job_id=?",
                    (status, last_error, utc_now_iso(), job_id),
                )
            else:
                self._conn.execute(
                    "UPDATE jobs SET status=?, last_error=?, updated_at=? WHERE job_id=?",
                    (status, last_error, utc_now_iso(), job_id),
                )
            self._conn.commit()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            record_count = self._conn.execute("SELECT COUNT(*) FROM records WHERE archived = 0").fetchone()[0]
            event_count = self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            pending_jobs = self._conn.execute("SELECT COUNT(*) FROM jobs WHERE status IN ('pending', 'retry_wait')").fetchone()[0]
            failed_jobs = self._conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'failed'").fetchone()[0]
        return {
            "active_records": record_count,
            "events": event_count,
            "pending_jobs": pending_jobs,
            "failed_jobs": failed_jobs,
            "last_capture_at": self.get_meta("last_capture_at"),
            "last_consolidation_at": self.get_meta("last_consolidation_at"),
            "index_updated_at": self.get_meta("index_updated_at"),
        }

    @staticmethod
    def _row_to_record(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "kind": row["kind"],
            "record_type": row["record_type"],
            "scope_id": row["scope_id"],
            "project_id": row["project_id"],
            "title": row["title"],
            "status": row["status"],
            "origin": row["origin"],
            "revision": row["revision"],
            "file_path": row["file_path"],
            "content_hash": row["content_hash"],
            "archived": bool(row["archived"]),
            "recorded_at": row["recorded_at"],
            "updated_at": row["updated_at"],
            "metadata": json.loads(row["metadata_json"]),
            "body": row["body_text"],
        }


def scope_permitted(record_scope: str, allowed_scopes: list[str]) -> bool:
    """Hierarchical scope check: 'personal' grants 'personal' and 'personal/*'."""
    rec = record_scope.strip().lower()
    for allowed in allowed_scopes:
        a = allowed.strip().lower()
        if a == "*" or rec == a or rec.startswith(a + "/"):
            return True
    return False


def _infer_kind(record_id: str, rec_type: str) -> str | None:
    if record_id.startswith("mem_"):
        return "memory"
    if record_id.startswith("task_") or rec_type == "task":
        return "task"
    if record_id.startswith("prj_") or rec_type == "project":
        return "project"
    if record_id.startswith("ses_") or rec_type == "session":
        return "session"
    if record_id.startswith("doc_") or rec_type == "document":
        return "document"
    return None
