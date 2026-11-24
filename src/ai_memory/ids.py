"""Opaque identifiers, timestamps, hashing, and safe filename helpers."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().strftime("%Y-%m-%dT%H:%M:%SZ")


def local_today_str(tz_name: str = "Asia/Kolkata") -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d")


def new_id(prefix: str) -> str:
    """Generate a compact, URL-safe, opaque permanent identifier."""
    token = secrets.token_hex(8)
    return f"{prefix}_{token}"


def canonical_json_bytes(obj: Any) -> bytes:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def canonical_request_hash(operation: str, payload: dict[str, Any]) -> str:
    return sha256_bytes(canonical_json_bytes({"operation": operation, "input": payload}))


_SLUG_RE = re.compile(r"[^a-zA-Z0-9]+")


def slugify(title: str, max_len: int = 48) -> str:
    slug = _SLUG_RE.sub("-", title.strip()).strip("-").lower()
    if not slug:
        slug = "untitled"
    return slug[:max_len].rstrip("-") or "untitled"
