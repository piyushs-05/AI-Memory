"""Task state machine, cycle detection, and daily/area task view generation (Design Section 12)."""

from __future__ import annotations

from typing import Any

from .errors import MemoryServiceError
from .ids import local_today_str, utc_now_iso

ALLOWED_TRANSITIONS: dict[str, set[str]] = {
    "inbox": {"inbox", "next", "cancelled"},
    "next": {"next", "in_progress", "blocked", "cancelled"},
    "in_progress": {"in_progress", "next", "blocked", "done", "cancelled"},
    "blocked": {"blocked", "next", "in_progress", "cancelled"},
    "done": {"done", "next"},
    "cancelled": {"cancelled", "next"},
}


def validate_task_transition(
    *,
    current_status: str,
    new_status: str,
    blocked_reason: str | None,
    depends_on: list[str],
    unresolved_dep_ids: set[str],
    has_completion_evidence: bool,
    reopen_reason: str | None,
) -> None:
    allowed = ALLOWED_TRANSITIONS.get(current_status, set())
    if new_status not in allowed:
        raise MemoryServiceError(
            "validation_failed",
            f"Invalid task transition: {current_status} -> {new_status}",
        )

    if current_status in ("done", "cancelled") and new_status == "next":
        if not reopen_reason or not reopen_reason.strip():
            raise MemoryServiceError(
                "validation_failed",
                "Reopening a done or cancelled task requires an explicit reason",
            )

    if new_status == "blocked":
        if not (blocked_reason and blocked_reason.strip()) and not (set(depends_on) & unresolved_dep_ids):
            raise MemoryServiceError(
                "validation_failed",
                "Blocked status requires a blocked_reason or at least one unresolved dependency",
            )

    if new_status == "done" and current_status != "done":
        if not has_completion_evidence:
            raise MemoryServiceError(
                "validation_failed",
                "Marking a task done requires supporting evidence (source_ids) or explicit user instruction",
            )


def check_dependency_cycles(task_id: str, new_deps: list[str], all_tasks: dict[str, list[str]]) -> None:
    """Reject any direct or transitive dependency cycle."""
    if task_id in new_deps:
        raise MemoryServiceError("validation_failed", "A task cannot depend on itself")

    graph = dict(all_tasks)
    graph[task_id] = list(new_deps)

    visited: set[str] = set()
    stack = list(new_deps)
    while stack:
        node = stack.pop()
        if node == task_id:
            raise MemoryServiceError("validation_failed", "Task dependency cycle detected")
        if node in visited:
            continue
        visited.add(node)
        stack.extend(graph.get(node, []))


def partition_daily_tasks(tasks: list[dict[str, Any]], today: str) -> dict[str, list[dict[str, Any]]]:
    """
    Partition active authoritative tasks into daily sections without modifying due/scheduled dates.
    """
    seen: set[str] = set()
    scheduled_today: list[dict[str, Any]] = []
    overdue: list[dict[str, Any]] = []
    in_progress: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    suggested_next: list[dict[str, Any]] = []

    for rec in tasks:
        tid = rec["id"]
        if tid in seen or rec.get("archived"):
            continue
        seen.add(tid)
        meta = rec["metadata"]
        status = meta.get("status", "inbox")
        if status in ("done", "cancelled"):
            continue

        due = meta.get("due_date")
        sched = meta.get("scheduled_date")

        if due and due < today:
            overdue.append(rec)
        if sched and sched == today:
            scheduled_today.append(rec)
        if status == "in_progress":
            in_progress.append(rec)
        elif status == "blocked":
            blocked.append(rec)
        elif status in ("next", "inbox"):
            suggested_next.append(rec)

    return {
        "overdue": overdue,
        "scheduled_today": scheduled_today,
        "in_progress": in_progress,
        "blocked": blocked,
        "suggested_next": suggested_next,
    }


def _fmt_task_line(rec: dict[str, Any]) -> str:
    meta = rec["metadata"]
    status = meta.get("status", "inbox")
    prio = meta.get("priority", "normal")
    bits = [f"`{rec['id']}`", f"[{status}]"]
    if prio != "normal":
        bits.append(f"({prio})")
    if meta.get("proposal"):
        bits.append("(proposal)")
    if meta.get("due_date"):
        bits.append(f"due:{meta['due_date']}")
    if meta.get("scheduled_date"):
        bits.append(f"sched:{meta['scheduled_date']}")
    if status == "blocked" and meta.get("blocked_reason"):
        bits.append(f"— blocked: {meta['blocked_reason']}")
    return f"- {rec['title']} ({' '.join(bits)})"


def render_today_markdown(tasks: list[dict[str, Any]], *, timezone: str = "Asia/Kolkata") -> str:
    today = local_today_str(timezone)
    sections = partition_daily_tasks(tasks, today)
    lines = [
        "---",
        'schema_version: "1.0"',
        "type: generated_view",
        f'generated_for_date: "{today}"',
        f'timezone: "{timezone}"',
        f'generated_at: "{utc_now_iso()}"',
        f"source_task_count: {len(tasks)}",
        "---",
        f"# Today — {today}",
        "",
        "> Derived view generated from authoritative task records. Do not edit manually.",
        "",
    ]

    for heading, key in (
        ("Overdue", "overdue"),
        ("Scheduled Today", "scheduled_today"),
        ("In Progress", "in_progress"),
        ("Blocked", "blocked"),
        ("Suggested Next & Inbox", "suggested_next"),
    ):
        lines.append(f"## {heading}")
        items = sections[key]
        if not items:
            lines.append("_None._")
        else:
            for rec in items:
                lines.append(_fmt_task_line(rec))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def render_area_tasks_markdown(area_name: str, tasks: list[dict[str, Any]]) -> str:
    lines = [
        "---",
        'schema_version: "1.0"',
        "type: generated_view",
        f'area: "{area_name}"',
        f'generated_at: "{utc_now_iso()}"',
        f"source_task_count: {len(tasks)}",
        "---",
        f"# {area_name} Tasks",
        "",
        "> Derived dashboard generated from authoritative task records.",
        "",
    ]

    by_status: dict[str, list[dict[str, Any]]] = {
        "in_progress": [],
        "next": [],
        "blocked": [],
        "inbox": [],
        "done": [],
        "cancelled": [],
    }
    for rec in tasks:
        if rec.get("archived"):
            continue
        st = rec["metadata"].get("status", "inbox")
        by_status.setdefault(st, []).append(rec)

    for heading, key in (
        ("In Progress", "in_progress"),
        ("Next", "next"),
        ("Blocked", "blocked"),
        ("Inbox", "inbox"),
        ("Done", "done"),
        ("Cancelled", "cancelled"),
    ):
        lines.append(f"## {heading}")
        items = by_status.get(key, [])
        if not items:
            lines.append("_None._")
        else:
            for rec in items:
                lines.append(_fmt_task_line(rec))
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
