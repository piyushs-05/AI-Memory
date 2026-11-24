"""Canonical filesystem storage with atomic fsync+replace and strict path safety."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .errors import MemoryServiceError
from .ids import slugify


def _fsync_dir(directory: Path) -> None:
    """Flush directory entry metadata on POSIX filesystems."""
    if os.name == "nt":
        return
    fd = os.open(str(directory), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write bytes to a sibling temp file, fsync, and atomically replace target."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        _fsync_dir(path.parent)
    except Exception:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        raise


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    raw = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=False) + "\n"
    atomic_write_text(path, raw)


def ensure_within(root: Path, target: Path) -> Path:
    """Ensure target path stays strictly inside root (prevents path traversal)."""
    resolved_root = root.resolve()
    resolved_target = target.resolve()
    try:
        resolved_target.relative_to(resolved_root)
    except ValueError as exc:
        raise MemoryServiceError("validation_failed", "Path escapes controlled storage root") from exc
    return resolved_target


def is_protected_policy_path(vault_dir: Path, target: Path) -> bool:
    rel = ensure_within(vault_dir, target).relative_to(vault_dir.resolve())
    parts = rel.parts
    if not parts:
        return True
    if parts[0] == "START-HERE.md":
        return True
    if parts[0] == "System":
        return True
    return False


def record_vault_path(
    config: MemoryConfig,
    *,
    kind: str,
    scope_id: str,
    record_id: str,
    title: str,
    area_folder: str | None = None,
    custom_subpath: list[str] | None = None,
) -> Path:
    """
    Determine canonical Obsidian-readable path for a new record under any dynamic area or nested scope.
    Examples:
      - scope_id='work', kind='memory' -> Work/Memories/<slug>--<id>.md
      - scope_id='work/internships/acme', kind='task' -> Work/Internships/Acme/Tasks/<slug>--<id>.md
      - scope_id='academics', custom_subpath=['Semester-6', 'OS'] -> Academics/Semester-6/OS/<slug>--<id>.md
    """
    from .scopes import normalize_scope_id, segment_to_folder_name

    norm = normalize_scope_id(scope_id)
    parts = norm.split("/")
    top = parts[0]
    nested = parts[1:]

    root_folder = area_folder or segment_to_folder_name(top)
    slug = slugify(title)
    filename = f"{slug}--{record_id}.md"

    rel_parts: list[str] = [root_folder]
    for seg in nested:
        rel_parts.append(segment_to_folder_name(seg))

    if custom_subpath:
        rel_parts.extend(custom_subpath)
    else:
        if kind == "memory":
            sub = "Knowledge" if (top == "shared" and not nested) else "Memories"
        elif kind == "task":
            sub = "Tasks" if top != "shared" else "Knowledge"
        elif kind == "session":
            sub = "Sessions" if top != "shared" else "Knowledge"
        elif kind == "document":
            sub = "Documents" if top != "shared" else "Knowledge"
        elif kind == "project":
            sub = "Projects" if top != "shared" else "Knowledge"
        else:
            raise MemoryServiceError("validation_failed", f"Unsupported record kind: {kind}")
        rel_parts.append(sub)

    target = config.vault_dir.joinpath(*rel_parts, filename)
    resolved = ensure_within(config.vault_dir, target)
    if is_protected_policy_path(config.vault_dir, resolved):
        raise MemoryServiceError("validation_failed", "Record path cannot target protected System/ policy folders")
    return resolved


def event_record_path(config: MemoryConfig, *, session_id: str, event_id: str, recorded_at: str) -> Path:
    """Immutable original event path partitioned by YYYY-MM and session_id."""
    month = recorded_at[:7] if len(recorded_at) >= 7 else "unknown"
    target = config.records_dir / "events" / month / session_id / f"{event_id}.json"
    return ensure_within(config.records_dir, target)
