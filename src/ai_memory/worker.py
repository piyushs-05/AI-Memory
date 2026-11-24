"""Bounded single-worker background processor for consolidation, PDF/OCR extraction, and reconciliation (R12, R18-R20)."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .access import ClientIdentity
from .documents import MAX_EXCERPT_CHARS_IN_NOTE, extract_document_content
from .frontmatter import content_hash, render_markdown
from .ids import utc_now, utc_now_iso
from .journal import FileMutationOp

if TYPE_CHECKING:
    from .core import MemoryCore


class BackgroundWorker:
    """
    One bounded background worker processing queued jobs one at a time with leases,
    retries, restart recovery, and periodic vault reconciliation.
    """

    def __init__(
        self,
        core: "MemoryCore",
        *,
        poll_interval: float = 2.0,
        reconcile_interval: float = 15.0,
        lease_seconds: int = 120,
    ) -> None:
        self._core = core
        self._poll_interval = poll_interval
        self._reconcile_interval = reconcile_interval
        self._lease_seconds = lease_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_reconcile = 0.0

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._core.index.recover_abandoned_jobs()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="ai-memory-worker", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                now = time.monotonic()
                if now - self._last_reconcile >= self._reconcile_interval:
                    self._core.index.reconcile_vault(self._core.config.vault_dir)
                    self._last_reconcile = now
                self.run_once()
            except Exception:
                pass
            self._stop_event.wait(self._poll_interval)

    def run_once(self) -> list[dict[str, Any]]:
        """Process all currently pending/retry_wait jobs one at a time."""
        outcomes: list[dict[str, Any]] = []
        jobs = self._core.index.list_jobs(status="pending") + self._core.index.list_jobs(status="retry_wait")
        for job in jobs:
            if job["attempts"] >= job["max_attempts"]:
                self._core.index.update_job_status(
                    job["job_id"], "failed", last_error=job.get("last_error") or "Max attempts reached"
                )
                continue
            jtype = job["job_type"]
            if jtype == "extract_document":
                outcomes.append(self._process_extract_document(job))
            elif jtype == "consolidate_session":
                outcomes.append(self._process_consolidate_session(job))
        return outcomes

    def _lease_until_iso(self) -> str:
        return (utc_now() + timedelta(seconds=self._lease_seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _process_extract_document(self, job: dict[str, Any]) -> dict[str, Any]:
        job_id = job["job_id"]
        doc_id = job["payload"]["document_id"]
        self._core.index.update_job_status(job_id, "running", inc_attempts=True, lease_until=self._lease_until_iso())

        rec = self._core.index.get_record(doc_id)
        if rec is None:
            self._core.index.update_job_status(job_id, "completed", last_error="Document no longer exists")
            return {"job_id": job_id, "status": "skipped"}

        meta = dict(rec["metadata"])
        result = extract_document_content(
            self._core.config,
            doc_id=doc_id,
            original_attachment_path=meta["original_attachment_path"],
            media_type=meta["media_type"],
        )

        status = result["extraction_status"]
        meta["extraction_status"] = status
        if result.get("page_count") is not None:
            meta["page_count"] = result["page_count"]
        meta["revision"] = int(meta["revision"]) + 1
        meta["updated_at"] = utc_now_iso()

        excerpt_lines = [
            f"# {meta['title']}",
            "",
            "## Metadata",
            f"- Original filename: `{meta['original_filename']}`",
            f"- Media type: `{meta['media_type']}`",
            f"- Checksum (SHA-256): `{meta['checksum_sha256']}`",
            f"- Extraction status: `{status}`",
        ]
        if meta.get("page_count"):
            excerpt_lines.append(f"- Page count: `{meta['page_count']}`")
        excerpt_lines.extend(["", "## Extracted Excerpts"])

        pages = result.get("pages") or []
        if pages:
            budget = MAX_EXCERPT_CHARS_IN_NOTE
            for p in pages:
                if budget <= 0:
                    break
                chunk = p["text"][:budget]
                excerpt_lines.append(f"\n### Page {p['page']}\n\n{chunk}")
                budget -= len(chunk)
        elif result.get("error"):
            excerpt_lines.append(f"_Extraction failed: {result['error']}_")
        else:
            excerpt_lines.append("_No text extracted._")

        new_body = "\n".join(excerpt_lines).rstrip() + "\n"
        rendered = render_markdown(meta, new_body)
        target_path = Path(rec["file_path"])

        try:
            self._core.journal.save_revision_snapshot(
                record_id=doc_id,
                revision=rec["revision"],
                actor="system:worker",
                reason="Document extraction update",
                metadata=rec["metadata"],
                body=rec["body"],
                raw_markdown=target_path.read_text(encoding="utf-8") if target_path.exists() else "",
            )
            entry = self._core.journal.execute(
                operation="extract_document",
                ops=[
                    FileMutationOp(
                        path=target_path,
                        action="write",
                        after_content=rendered,
                        expected_before_hash=rec["content_hash"],
                    )
                ],
                metadata={"document_id": doc_id, "job_id": job_id},
            )
            self._core.journal.mark_projecting(entry["mutation_id"])
            self._core.index.upsert_record(
                record_id=doc_id,
                kind="document",
                meta=meta,
                body=new_body,
                file_path=target_path,
                raw_hash=content_hash(rendered),
            )
            self._core.journal.mark_completed(entry["mutation_id"])
            final_job_status = "failed" if status == "failed" else "completed"
            self._core.index.update_job_status(job_id, final_job_status, last_error=result.get("error"))
            return {"job_id": job_id, "document_id": doc_id, "extraction_status": status}
        except Exception as exc:
            next_state = "failed" if job["attempts"] + 1 >= job["max_attempts"] else "retry_wait"
            self._core.index.update_job_status(job_id, next_state, last_error=str(exc))
            return {"job_id": job_id, "document_id": doc_id, "error": str(exc)}

    def _process_consolidate_session(self, job: dict[str, Any]) -> dict[str, Any]:
        scope_id = job["payload"]["scope_id"]
        worker_client = ClientIdentity(
            client_id="system_worker",
            label="background-worker",
            allowed_scopes=(scope_id,),
            can_write=True,
            can_forget=False,
        )
        ctx_resp = self._core.dispatch(
            client=worker_client,
            operation="open_context",
            payload={"purpose": f"background consolidation {job['job_id']}", "scopes": [scope_id]},
        )
        if not ctx_resp.get("ok"):
            self._core.index.update_job_status(job["job_id"], "failed", last_error="Could not open worker context")
            return {"job_id": job["job_id"], "error": "Could not open worker context"}
        ctx = self._core.index.get_context(ctx_resp["data"]["context_id"])
        assert ctx is not None
        return self._core._run_single_consolidation_job(job=job, client=worker_client, ctx=ctx)
