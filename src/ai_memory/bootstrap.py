"""Idempotent initialization of memory-home directories, schemas, dynamic areas, and vault bootstrap files."""

from __future__ import annotations

import shutil

from .config import MemoryConfig
from .scopes import AreaRegistry

SYSTEM_VAULT_SUBDIRS = [
    "System/Rules",
    "System/Templates",
]


def init_memory_home(config: MemoryConfig, *, overwrite_policy: bool = False) -> dict[str, list[str]]:
    """
    Create the vault layout idempotently from config/areas.json without overwriting user files.
    New areas and nested subfolders added to config/areas.json are created automatically.
    """
    created_dirs: list[str] = []
    copied_files: list[str] = []
    skipped_files: list[str] = []

    for base in (
        config.data_root,
        config.vault_dir,
        config.records_dir,
        config.attachments_dir,
        config.config_dir,
        config.schemas_dir,
        config.journal_dir,
        config.runtime_dir,
    ):
        if not base.exists():
            base.mkdir(parents=True, mode=0o700, exist_ok=True)
            created_dirs.append(str(base))

    for rel in SYSTEM_VAULT_SUBDIRS:
        target = config.vault_dir / rel
        if not target.exists():
            target.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(target))

    # Initialize dynamic areas registry (defaults to shared, work, personal; extensible anytime)
    areas = AreaRegistry(config).ensure_initialized()
    for _slug, area_cfg in areas.items():
        area_dir = config.vault_dir / area_cfg["folder"]
        if not area_dir.exists():
            area_dir.mkdir(parents=True, exist_ok=True)
            created_dirs.append(str(area_dir))
        for sub in area_cfg.get("subfolders") or []:
            sub_dir = area_dir / sub
            if not sub_dir.exists():
                sub_dir.mkdir(parents=True, exist_ok=True)
                created_dirs.append(str(sub_dir))

    # Copy JSON schemas into data_root/schemas
    if config.repo_schemas_dir.is_dir():
        for src in sorted(config.repo_schemas_dir.glob("*.json")):
            dst = config.schemas_dir / src.name
            if not dst.exists() or overwrite_policy:
                shutil.copy2(src, dst)
                copied_files.append(str(dst))
            else:
                skipped_files.append(str(dst))

    # Copy vault bootstrap files (never overwrite user-maintained files like PROFILE.md)
    if config.repo_bootstrap_dir.is_dir():
        for src in sorted(config.repo_bootstrap_dir.rglob("*")):
            if src.is_dir():
                continue
            rel_path = src.relative_to(config.repo_bootstrap_dir)
            dst = config.vault_dir / rel_path
            dst.parent.mkdir(parents=True, exist_ok=True)
            is_policy = rel_path.parts[0] == "System" or rel_path.name == "START-HERE.md"
            if not dst.exists() or (overwrite_policy and is_policy):
                shutil.copy2(src, dst)
                copied_files.append(str(dst))
            else:
                skipped_files.append(str(dst))

    return {
        "created_dirs": created_dirs,
        "copied_files": copied_files,
        "skipped_files": skipped_files,
    }
