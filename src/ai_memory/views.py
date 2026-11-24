"""Scope-filtered bootstrap packet and derived owner SUMMARY.md generation."""

from __future__ import annotations

from typing import Any

from .config import MemoryConfig
from .ids import local_today_str, utc_now_iso
from .index import MemoryIndex, scope_permitted
from .tasks import partition_daily_tasks


def build_bootstrap_packet(
    *,
    config: MemoryConfig,
    index: MemoryIndex,
    allowed_scopes: list[str],
    project_id: str | None = None,
) -> dict[str, Any]:
    """
    Assemble a compact, strictly scope-filtered bootstrap packet from permitted source records.
    Never reads a mixed owner SUMMARY.md and attempts to redact it afterward.
    """
    permitted_memories = index.list_records(kind="memory", scopes=allowed_scopes, include_archived=False)
    permitted_tasks = index.list_records(kind="task", scopes=allowed_scopes, include_archived=False)
    permitted_projects = index.list_records(kind="project", scopes=allowed_scopes, include_archived=False)

    if project_id:
        project_memories = [m for m in permitted_memories if m.get("project_id") == project_id]
        project_tasks = [t for t in permitted_tasks if t.get("project_id") == project_id]
    else:
        project_memories = permitted_memories
        project_tasks = permitted_tasks

    today = local_today_str(config.timezone)
    daily = partition_daily_tasks(project_tasks, today)

    profile_excerpt: str | None = None
    profile_path = config.vault_dir / "PROFILE.md"
    if scope_permitted("shared", allowed_scopes) and profile_path.is_file():
        profile_excerpt = profile_path.read_text(encoding="utf-8")[:2000]

    return {
        "introduction": (
            "Portable AI Memory Service. Follow System/Rules/*: cite sources on all writes, "
            "pass idempotency_key on mutations, respect scope boundaries, and do not promote "
            "temporary states or inferences to lasting facts."
        ),
        "timezone": config.timezone,
        "local_date": today,
        "allowed_scopes": allowed_scopes,
        "profile_excerpt": profile_excerpt,
        "active_projects": [
            {"id": p["id"], "title": p["title"], "scope_id": p["scope_id"], "status": p["status"]}
            for p in permitted_projects[:10]
        ],
        "recent_memories": [
            {
                "id": m["id"],
                "type": m["record_type"],
                "title": m["title"],
                "scope_id": m["scope_id"],
                "status": m["status"],
                "revision": m["revision"],
            }
            for m in project_memories[:15]
        ],
        "daily_tasks": {
            section: [
                {
                    "id": t["id"],
                    "title": t["title"],
                    "scope_id": t["scope_id"],
                    "status": t["status"],
                    "priority": t["metadata"].get("priority", "normal"),
                    "due_date": t["metadata"].get("due_date"),
                    "scheduled_date": t["metadata"].get("scheduled_date"),
                }
                for t in items[:10]
            ]
            for section, items in daily.items()
        },
        "navigation": [
            "START-HERE.md",
            "System/Rules/READ.md",
            "System/Rules/CREATE.md",
            "System/Rules/UPDATE.md",
            "System/Rules/DELETE.md",
            "System/Rules/MEMORY.md",
            "System/Rules/TASKS.md",
        ],
    }


def render_owner_summary_markdown(index: MemoryIndex) -> str:
    """Generate root SUMMARY.md for the owner from current non-archived records."""
    memories = index.list_records(kind="memory", include_archived=False)
    tasks = index.list_records(kind="task", include_archived=False)
    projects = index.list_records(kind="project", include_archived=False)

    open_tasks = [t for t in tasks if t["status"] not in ("done", "cancelled")]
    source_revs = [{"id": r["id"], "revision": r["revision"]} for r in (projects + memories)[:50]]

    lines = [
        "---",
        'schema_version: "1.0"',
        "type: generated_view",
        f'generated_at: "{utc_now_iso()}"',
        f"active_projects: {len(projects)}",
        f"active_memories: {len(memories)}",
        f"open_tasks: {len(open_tasks)}",
        f"covered_sources_count: {len(source_revs)}",
        "---",
        "# Memory Summary",
        "",
        "> Derived owner overview generated from authoritative records. Restricted clients receive scope-filtered packets instead of this file.",
        "",
        "## Active Projects",
    ]
    if not projects:
        lines.append("_None recorded._")
    else:
        for p in projects[:20]:
            lines.append(f"- **{p['title']}** (`{p['id']}`, scope `{p['scope_id']}`, status `{p['status']}`)")

    lines.extend(["", "## Recent Current Memories"])
    current_memories = [m for m in memories if m["status"] in ("current", "disputed", "uncertain")]
    if not current_memories:
        lines.append("_None recorded._")
    else:
        for m in current_memories[:25]:
            lines.append(
                f"- **{m['title']}** (`{m['id']}`, {m['record_type']}/{m['status']}, rev {m['revision']}, scope `{m['scope_id']}`)"
            )

    lines.extend(["", "## Open Tasks Overview"])
    lines.append(f"- Open tasks: **{len(open_tasks)}** (see [[TODAY]] and area `TASKS.md` views)")
    lines.append("")
    return "\n".join(lines)
