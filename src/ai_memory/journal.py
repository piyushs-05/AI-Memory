"""Durable mutation journal and crash-recovery state machine (Design Section 10)."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .errors import MemoryServiceError
from .frontmatter import content_hash
from .ids import new_id, utc_now_iso
from .storage import atomic_write_json, atomic_write_text, ensure_within, is_protected_policy_path


@dataclass(frozen=True)
class FileMutationOp:
    """A single file write or delete inside a logical mutation."""

    path: Path
    action: str  # "write" | "delete"
    after_content: str | None = None
    expected_before_hash: str | None = None
    allow_overwrite_missing_hash: bool = False


class MutationJournal:
    """
    Serialized mutation coordinator implementing:
    Received -> Validating -> Prepared -> Applying -> Committed -> Projecting -> Completed
    with crash recovery (Recovering -> Applying | Aborted | RepairRequired).
    """

    def __init__(self, config: MemoryConfig) -> None:
        self._config = config
        self._lock = threading.RLock()
        self._config.journal_dir.mkdir(parents=True, exist_ok=True)

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def _journal_path(self, mutation_id: str) -> Path:
        return self._config.journal_dir / f"{mutation_id}.json"

    def _persist(self, entry: dict[str, Any]) -> None:
        entry["updated_at"] = utc_now_iso()
        atomic_write_json(self._journal_path(entry["mutation_id"]), entry)

    def execute(
        self,
        *,
        operation: str,
        ops: list[FileMutationOp],
        metadata: dict[str, Any] | None = None,
        allow_protected: bool = False,
        fail_after_state: str | None = None,
    ) -> dict[str, Any]:
        """
        Execute a multi-file mutation with durable journaling and external-edit detection.
        `fail_after_state` is an optional test hook ("Prepared" or "Applying") to simulate crashes.
        """
        with self._lock:
            mutation_id = new_id("mut")
            entry: dict[str, Any] = {
                "mutation_id": mutation_id,
                "operation": operation,
                "state": "Received",
                "created_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "metadata": metadata or {},
                "targets": [],
                "error": None,
            }

            # 1. Validating: check path safety, protected policy rules, and external-edit hashes
            entry["state"] = "Validating"
            prepared_targets: list[dict[str, Any]] = []
            for op in ops:
                resolved = ensure_within(self._config.data_root, op.path)
                if resolved.is_relative_to(self._config.vault_dir.resolve()):
                    if not allow_protected and is_protected_policy_path(self._config.vault_dir, resolved):
                        raise MemoryServiceError(
                            "validation_failed",
                            "Protected policy documents cannot be modified by ordinary memory operations",
                        )

                exists = resolved.exists()
                before_content = resolved.read_text(encoding="utf-8") if exists else None
                before_hash = content_hash(before_content) if before_content is not None else None

                if op.expected_before_hash is not None:
                    if before_hash != op.expected_before_hash:
                        raise MemoryServiceError(
                            "revision_conflict",
                            "File changed on disk outside the service; manual edit detected and preserved",
                            details={
                                "expected_hash": op.expected_before_hash,
                                "actual_hash": before_hash,
                            },
                        )
                elif exists and op.action == "write" and not op.allow_overwrite_missing_hash:
                    raise MemoryServiceError(
                        "revision_conflict",
                        "Target file already exists without an expected content hash",
                    )

                after_hash = content_hash(op.after_content) if op.after_content is not None else None
                prepared_targets.append(
                    {
                        "path": str(resolved),
                        "action": op.action,
                        "before_exists": exists,
                        "before_hash": before_hash,
                        "after_hash": after_hash,
                        "after_content": op.after_content,
                    }
                )

            # 2. Prepared: durably persist journal before touching any target file
            entry["targets"] = prepared_targets
            entry["state"] = "Prepared"
            self._persist(entry)

            if fail_after_state == "Prepared":
                raise RuntimeError(f"Simulated crash after Prepared for {mutation_id}")

            # 3. Applying: write/delete each target atomically
            entry["state"] = "Applying"
            self._persist(entry)
            try:
                self._apply_targets(prepared_targets, fail_midway=(fail_after_state == "Applying"))
            except Exception as exc:
                entry["error"] = str(exc)
                self._persist(entry)
                raise

            # 4. Committed: canonical files are durable
            entry["state"] = "Committed"
            self._persist(entry)
            return entry

    def mark_projecting(self, mutation_id: str) -> None:
        with self._lock:
            path = self._journal_path(mutation_id)
            if not path.exists():
                return
            entry = json.loads(path.read_text(encoding="utf-8"))
            entry["state"] = "Projecting"
            self._persist(entry)

    def mark_completed(self, mutation_id: str) -> None:
        with self._lock:
            path = self._journal_path(mutation_id)
            if not path.exists():
                return
            entry = json.loads(path.read_text(encoding="utf-8"))
            entry["state"] = "Completed"
            # Strip bulky file bodies once completed to keep journal compact
            for target in entry.get("targets", []):
                target.pop("after_content", None)
            self._persist(entry)

    def mark_projection_pending(self, mutation_id: str, error: str) -> None:
        with self._lock:
            path = self._journal_path(mutation_id)
            if not path.exists():
                return
            entry = json.loads(path.read_text(encoding="utf-8"))
            entry["state"] = "ProjectionPending"
            entry["error"] = error
            self._persist(entry)

    def _apply_targets(self, targets: list[dict[str, Any]], *, fail_midway: bool = False) -> None:
        for idx, target in enumerate(targets):
            p = Path(target["path"])
            if target["action"] == "write":
                atomic_write_text(p, target["after_content"])
            elif target["action"] == "delete":
                if p.exists():
                    p.unlink()
            if fail_midway and idx == 0:
                raise RuntimeError("Simulated crash during Applying")

    def recover_incomplete(self) -> dict[str, list[str]]:
        """
        Replay incomplete journal entries on startup.
        - Prepared / Applying: deterministically roll forward if disk matches before_hash or after_hash;
          otherwise quarantine as RepairRequired without overwriting manual edits.
        - Committed / Projecting / ProjectionPending: report for projection refresh.
        """
        with self._lock:
            recovered: list[str] = []
            needs_projection: list[str] = []
            repair_required: list[str] = []

            for jpath in sorted(self._config.journal_dir.glob("mut_*.json")):
                entry = json.loads(jpath.read_text(encoding="utf-8"))
                state = entry.get("state")
                mut_id = entry["mutation_id"]

                if state in ("Completed", "Aborted", "Rejected", "Conflict"):
                    continue
                if state == "RepairRequired":
                    repair_required.append(mut_id)
                    continue
                if state in ("Committed", "Projecting", "ProjectionPending"):
                    needs_projection.append(mut_id)
                    continue
                if state in ("Prepared", "Applying", "Recovering"):
                    entry["state"] = "Recovering"
                    self._persist(entry)
                    safe_to_roll_forward = True
                    for target in entry.get("targets", []):
                        p = Path(target["path"])
                        cur_hash = content_hash(p.read_text(encoding="utf-8")) if p.exists() else None
                        if target["action"] == "write":
                            if cur_hash not in (target["before_hash"], target["after_hash"]):
                                safe_to_roll_forward = False
                                break
                        elif target["action"] == "delete":
                            if p.exists() and cur_hash != target["before_hash"]:
                                safe_to_roll_forward = False
                                break

                    if not safe_to_roll_forward:
                        entry["state"] = "RepairRequired"
                        entry["error"] = "Disk diverged from journal during recovery; quarantined to preserve edits"
                        self._persist(entry)
                        repair_required.append(mut_id)
                        continue

                    entry["state"] = "Applying"
                    self._persist(entry)
                    self._apply_targets(entry.get("targets", []))
                    entry["state"] = "Committed"
                    entry["error"] = None
                    self._persist(entry)
                    recovered.append(mut_id)
                    needs_projection.append(mut_id)

            return {
                "recovered": recovered,
                "needs_projection": needs_projection,
                "repair_required": repair_required,
            }

    def list_repair_required(self) -> list[str]:
        out: list[str] = []
        for jpath in sorted(self._config.journal_dir.glob("mut_*.json")):
            entry = json.loads(jpath.read_text(encoding="utf-8"))
            if entry.get("state") == "RepairRequired":
                out.append(entry["mutation_id"])
        return out
