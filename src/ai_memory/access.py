"""Credential management, scope intersection, and authorization helpers (Design Section 8)."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .errors import MemoryServiceError
from .ids import new_id, utc_now_iso
from .index import scope_permitted
from .storage import atomic_write_json


@dataclass(frozen=True)
class ClientIdentity:
    client_id: str
    label: str
    allowed_scopes: tuple[str, ...]
    can_write: bool = True
    can_forget: bool = False


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


class AccessPolicy:
    """Loads hashed bearer tokens from config/credentials.json (kept outside vault/)."""

    def __init__(self, config: MemoryConfig) -> None:
        self._config = config

    @property
    def credentials_path(self) -> Path:
        return self._config.credentials_path

    def _load_raw(self) -> dict[str, Any]:
        if not self.credentials_path.exists():
            return {"version": "1", "clients": []}
        return json.loads(self.credentials_path.read_text(encoding="utf-8"))

    def create_token(
        self,
        *,
        label: str,
        allowed_scopes: list[str],
        can_write: bool = True,
        can_forget: bool = False,
    ) -> tuple[ClientIdentity, str]:
        """Create a new client credential; returns (identity, plaintext_token_once)."""
        raw = self._load_raw()
        client_id = new_id("cli")
        token = f"aim_{secrets.token_urlsafe(32)}"
        entry = {
            "client_id": client_id,
            "label": label,
            "token_sha256": hash_token(token),
            "allowed_scopes": sorted(set(allowed_scopes)),
            "can_write": bool(can_write),
            "can_forget": bool(can_forget),
            "created_at": utc_now_iso(),
            "revoked": False,
        }
        raw.setdefault("clients", []).append(entry)
        self.credentials_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        atomic_write_json(self.credentials_path, raw)
        if os.name != "nt":
            try:
                os.chmod(self.credentials_path, 0o600)
            except OSError:
                pass
        return (
            ClientIdentity(
                client_id=client_id,
                label=label,
                allowed_scopes=tuple(entry["allowed_scopes"]),
                can_write=entry["can_write"],
                can_forget=entry["can_forget"],
            ),
            token,
        )

    def authenticate(self, token: str | None) -> ClientIdentity:
        if not token:
            raise MemoryServiceError("authentication_required", "Missing bearer token")
        digest = hash_token(token.strip())
        raw = self._load_raw()
        for entry in raw.get("clients", []):
            if entry.get("revoked"):
                continue
            expected = entry.get("token_sha256", "")
            if expected and hmac.compare_digest(digest, expected):
                return ClientIdentity(
                    client_id=entry["client_id"],
                    label=entry["label"],
                    allowed_scopes=tuple(entry["allowed_scopes"]),
                    can_write=bool(entry.get("can_write", True)),
                    can_forget=bool(entry.get("can_forget", False)),
                )
        raise MemoryServiceError("authentication_required", "Invalid or revoked bearer token")


def intersect_scopes(credential_scopes: tuple[str, ...] | list[str], requested_scopes: list[str] | None) -> list[str]:
    """
    effective access = credential permissions intersect selected context permissions.
    Never grants a scope not permitted by the credential.
    """
    cred = [s.strip().lower() for s in credential_scopes if s.strip()]
    if not requested_scopes:
        return cred
    effective: list[str] = []
    for req in requested_scopes:
        r = req.strip().lower()
        if not r:
            continue
        if scope_permitted(r, cred):
            effective.append(r)
        elif any(c.startswith(r + "/") for c in cred):
            for c in cred:
                if c.startswith(r + "/"):
                    effective.append(c)
    return sorted(set(effective))


def require_scope(target_scope: str, allowed_scopes: list[str], *, hide_existence: bool = False) -> None:
    if not scope_permitted(target_scope, allowed_scopes):
        if hide_existence:
            raise MemoryServiceError("not_found_or_unavailable", "Record not found or unavailable")
        raise MemoryServiceError(
            "validation_failed",
            f"Scope '{target_scope}' is not permitted in the active context",
        )
