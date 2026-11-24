"""Dynamic area and nested scope registry backed by config/areas.json."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .errors import MemoryServiceError
from .storage import atomic_write_json, atomic_write_text, ensure_within

_SEGMENT_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_FOLDER_SEGMENT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.-]{0,63}$")
_RESERVED_AREAS = frozenset({"system", "records", "runtime", "journal", "config", "schemas"})

DEFAULT_AREAS: dict[str, dict[str, Any]] = {
    "shared": {
        "folder": "Shared",
        "description": "Explicitly shared preferences and cross-cutting knowledge.",
        "subfolders": ["Knowledge"],
        "has_tasks_dashboard": False,
    },
    "work": {
        "folder": "Work",
        "description": "Internships, corporate activity, work projects, sessions, memories, tasks, and documents.",
        "subfolders": ["Tasks", "Internships", "Corporate", "Projects", "Sessions", "Memories", "Documents"],
        "has_tasks_dashboard": True,
    },
    "personal": {
        "folder": "Personal",
        "description": "Personal life, skills, personal projects, open-source, sessions, memories, tasks, and documents.",
        "subfolders": ["Tasks", "Life", "Skills", "Projects", "Open-source", "Sessions", "Memories", "Documents"],
        "has_tasks_dashboard": True,
    },
}


def normalize_scope_id(scope_id: str) -> str:
    """Validate and normalize a hierarchical scope_id (e.g. 'personal/hobbies/robotics')."""
    raw = (scope_id or "").strip().strip("/").lower()
    if not raw:
        raise MemoryServiceError("validation_failed", "scope_id must be non-empty")
    parts = raw.split("/")
    if len(parts) > 6:
        raise MemoryServiceError("validation_failed", "scope_id nesting depth exceeds maximum of 6")
    if parts[0] in _RESERVED_AREAS:
        raise MemoryServiceError("validation_failed", f"Top-level scope '{parts[0]}' is reserved")
    for part in parts:
        if not _SEGMENT_RE.match(part):
            raise MemoryServiceError(
                "validation_failed",
                f"Invalid scope segment '{part}'; use lowercase letters, digits, '-' or '_'",
            )
    return "/".join(parts)


def segment_to_folder_name(segment: str) -> str:
    """Convert a lowercase scope segment like 'open-source' into a readable Obsidian folder name."""
    return "-".join(piece.capitalize() for piece in segment.split("-") if piece)


def sanitize_subpath(subpath: str | None) -> list[str]:
    """Validate an optional relative folder subpath inside a scope (e.g. 'Hobbies/Photography')."""
    if not subpath:
        return []
    cleaned = subpath.replace("\\", "/").strip().strip("/")
    if not cleaned:
        return []
    parts = [p.strip() for p in cleaned.split("/") if p.strip()]
    if len(parts) > 6:
        raise MemoryServiceError("validation_failed", "Custom folder depth exceeds maximum of 6")
    for part in parts:
        if part in (".", "..") or not _FOLDER_SEGMENT_RE.match(part):
            raise MemoryServiceError("validation_failed", f"Invalid folder segment: '{part}'")
    return parts


class AreaRegistry:
    """
    Manages dynamic top-level areas in config/areas.json.
    New areas and nested subfolders can be added at runtime without modifying code.
    """

    def __init__(self, config: MemoryConfig) -> None:
        self._config = config

    @property
    def path(self) -> Path:
        return self._config.areas_path

    def ensure_initialized(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            atomic_write_json(self.path, {"version": "1", "areas": DEFAULT_AREAS})
        return self.list_areas()

    def list_areas(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return dict(DEFAULT_AREAS)
        raw = json.loads(self.path.read_text(encoding="utf-8"))
        return dict(raw.get("areas") or {})

    def get_area_folder(self, top_scope: str) -> str:
        """Resolve a top-level scope slug to its Obsidian folder name (works even for unregistered scopes)."""
        areas = self.list_areas()
        if top_scope in areas and areas[top_scope].get("folder"):
            return str(areas[top_scope]["folder"])
        return segment_to_folder_name(top_scope)

    def register_area(
        self,
        *,
        slug: str,
        folder: str | None = None,
        description: str = "",
        subfolders: list[str] | None = None,
        has_tasks_dashboard: bool = True,
        create_in_vault: bool = True,
    ) -> dict[str, Any]:
        """Add or update a top-level area dynamically and create its vault directories/INDEX.md."""
        norm = normalize_scope_id(slug)
        if "/" in norm:
            raise MemoryServiceError("validation_failed", "Top-level area slug must not contain '/'")

        folder_name = (folder or segment_to_folder_name(norm)).strip()
        if not _FOLDER_SEGMENT_RE.match(folder_name) or folder_name.lower() in _RESERVED_AREAS:
            raise MemoryServiceError("validation_failed", f"Invalid area folder name: '{folder_name}'")

        default_subs = ["Tasks", "Projects", "Sessions", "Memories", "Documents"]
        chosen_subs = subfolders if subfolders is not None else default_subs
        validated_subs: list[str] = []
        for sub in chosen_subs:
            parts = sanitize_subpath(sub)
            if parts:
                validated_subs.append("/".join(parts))

        areas = self.ensure_initialized()
        existing = areas.get(norm, {})
        target_folder = existing.get("folder") or folder_name

        # Prevent case-insensitive folder collisions across distinct areas (R15)
        for other_slug, other_cfg in areas.items():
            if other_slug == norm:
                continue
            other_folder = str(other_cfg.get("folder") or "")
            if other_folder.lower() == target_folder.lower():
                raise MemoryServiceError(
                    "validation_failed",
                    f"Folder '{target_folder}' is already owned by area '{other_slug}'",
                )

        # Reject silently adopting an unmanaged existing top-level directory for a brand-new area
        if norm not in areas and create_in_vault:
            candidate_dir = self._config.vault_dir / target_folder
            if candidate_dir.exists():
                raise MemoryServiceError(
                    "validation_failed",
                    f"Directory '{target_folder}' already exists in vault; choose a distinct folder name",
                )

        merged_subs = list(dict.fromkeys(list(existing.get("subfolders") or []) + validated_subs))
        entry = {
            "folder": target_folder,
            "description": description or existing.get("description") or f"Area for {norm}.",
            "subfolders": merged_subs,
            "has_tasks_dashboard": has_tasks_dashboard,
        }
        areas[norm] = entry
        atomic_write_json(self.path, {"version": "1", "areas": areas})

        if create_in_vault:
            area_dir = ensure_within(self._config.vault_dir, self._config.vault_dir / entry["folder"])
            area_dir.mkdir(parents=True, exist_ok=True)
            for sub in merged_subs:
                sub_dir = ensure_within(area_dir, area_dir / sub)
                sub_dir.mkdir(parents=True, exist_ok=True)
            index_path = area_dir / "INDEX.md"
            if not index_path.exists():
                sub_lines = "\n".join(f"- `{s}/`" for s in merged_subs)
                tasks_line = f"- [[{entry['folder']}/TASKS|{entry['folder']} Tasks Dashboard]] (generated)\n" if has_tasks_dashboard else ""
                atomic_write_text(
                    index_path,
                    (
                        f"---\nschema_version: \"1.0\"\ntype: index\nscope_id: {norm}\n---\n"
                        f"# {entry['folder']}\n\n{entry['description']}\n\n"
                        f"{tasks_line}{sub_lines}\n"
                    ),
                )

        return {"slug": norm, **entry}
