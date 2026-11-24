"""Deterministic YAML frontmatter + Markdown parsing and rendering."""

from __future__ import annotations

from typing import Any

import yaml

from .errors import MemoryServiceError
from .ids import sha256_text


def parse_markdown(text: str) -> tuple[dict[str, Any], str]:
    """Parse a Markdown document with YAML frontmatter into (metadata, body)."""
    normalized = text.replace("\r\n", "\n")
    if not normalized.startswith("---\n"):
        return {}, normalized

    end_idx = normalized.find("\n---\n", 4)
    if end_idx == -1:
        if normalized.endswith("\n---"):
            end_idx = len(normalized) - 4
            body = ""
        else:
            raise MemoryServiceError("validation_failed", "Unterminated YAML frontmatter block")
    else:
        body = normalized[end_idx + 5 :]

    raw_yaml = normalized[4:end_idx]
    try:
        meta = yaml.safe_load(raw_yaml)
    except yaml.YAMLError as exc:
        raise MemoryServiceError("validation_failed", f"Invalid YAML frontmatter: {exc}") from exc

    if meta is None:
        meta = {}
    if not isinstance(meta, dict):
        raise MemoryServiceError("validation_failed", "YAML frontmatter must be a mapping")
    return meta, body.lstrip("\n")


def render_markdown(meta: dict[str, Any], body: str) -> str:
    """Render metadata + body deterministically with LF line endings."""
    dumped = yaml.safe_dump(
        meta,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
        width=120,
    ).strip()
    clean_body = body.replace("\r\n", "\n").strip()
    if clean_body:
        return f"---\n{dumped}\n---\n{clean_body}\n"
    return f"---\n{dumped}\n---\n"


def content_hash(text: str) -> str:
    return sha256_text(text.replace("\r\n", "\n"))
