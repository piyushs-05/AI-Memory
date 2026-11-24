"""Configuration and bounds for the Portable AI Memory service."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MemoryConfig:
    data_root: Path
    repo_root: Path
    timezone: str = "Asia/Kolkata"
    policy_version: str = "1"
    api_version: str = "1"
    context_ttl_seconds: int = 12 * 3600
    idempotency_retention_days: int = 30
    max_event_batch_size: int = 100
    max_body_chars: int = 200_000
    max_search_limit: int = 50
    max_attachment_bytes: int = 25 * 1024 * 1024  # 25 MiB
    http_host: str = "127.0.0.1"
    http_port: int = 8765

    @property
    def vault_dir(self) -> Path:
        return self.data_root / "vault"

    @property
    def records_dir(self) -> Path:
        return self.data_root / "records"

    @property
    def config_dir(self) -> Path:
        return self.data_root / "config"

    @property
    def schemas_dir(self) -> Path:
        return self.data_root / "schemas"

    @property
    def journal_dir(self) -> Path:
        return self.data_root / "journal"

    @property
    def runtime_dir(self) -> Path:
        return self.data_root / "runtime"

    @property
    def db_path(self) -> Path:
        return self.runtime_dir / "memory.sqlite3"

    @property
    def credentials_path(self) -> Path:
        return self.config_dir / "credentials.json"

    @property
    def areas_path(self) -> Path:
        return self.config_dir / "areas.json"

    @property
    def attachments_dir(self) -> Path:
        return self.data_root / "records" / "attachments"

    @property
    def repo_schemas_dir(self) -> Path:
        return self.repo_root / "schemas"

    @property
    def repo_bootstrap_dir(self) -> Path:
        return self.repo_root / "bootstrap"

    @classmethod
    def from_env(cls, data_root: str | Path | None = None, repo_root: str | Path | None = None) -> "MemoryConfig":
        resolved_data = Path(
            data_root or os.environ.get("AI_MEMORY_HOME", "./memory-home")
        ).expanduser().resolve()
        if repo_root is not None:
            resolved_repo = Path(repo_root).expanduser().resolve()
        else:
            # src/ai_memory/config.py -> repo root is 2 parents up from src
            resolved_repo = Path(__file__).resolve().parents[2]
        return cls(
            data_root=resolved_data,
            repo_root=resolved_repo,
            timezone=os.environ.get("AI_MEMORY_TIMEZONE", "Asia/Kolkata"),
            http_host=os.environ.get("AI_MEMORY_HOST", "127.0.0.1"),
            http_port=int(os.environ.get("AI_MEMORY_PORT", "8765")),
        )
