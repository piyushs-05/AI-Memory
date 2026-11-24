"""JSON Schema Draft 2020-12 loader and validator."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator

from .errors import MemoryServiceError


class SchemaRegistry:
    """Loads and caches JSON Schema validators for all record kinds."""

    SCHEMA_FILES = {
        "event": "event.schema.json",
        "memory": "memory.schema.json",
        "task": "task.schema.json",
        "project": "project.schema.json",
        "session": "session.schema.json",
        "document": "document.schema.json",
    }

    def __init__(self, primary_dir: Path, fallback_dir: Path | None = None) -> None:
        self._primary_dir = primary_dir
        self._fallback_dir = fallback_dir
        self._validators: dict[str, Draft202012Validator] = {}

    def _resolve_path(self, filename: str) -> Path:
        candidate = self._primary_dir / filename
        if candidate.is_file():
            return candidate
        if self._fallback_dir is not None:
            fallback = self._fallback_dir / filename
            if fallback.is_file():
                return fallback
        raise FileNotFoundError(f"Schema file not found: {filename}")

    def get_validator(self, kind: str) -> Draft202012Validator:
        if kind not in self.SCHEMA_FILES:
            raise MemoryServiceError("validation_failed", f"Unknown schema kind: {kind}")
        if kind not in self._validators:
            path = self._resolve_path(self.SCHEMA_FILES[kind])
            schema = json.loads(path.read_text(encoding="utf-8"))
            self._validators[kind] = Draft202012Validator(schema)
        return self._validators[kind]

    def validate(self, kind: str, instance: dict[str, Any]) -> None:
        validator = self.get_validator(kind)
        errors = sorted(validator.iter_errors(instance), key=lambda e: list(e.path))
        if errors:
            first = errors[0]
            field = ".".join(str(p) for p in first.path) or "<root>"
            raise MemoryServiceError(
                "validation_failed",
                f"Schema validation failed for {kind} at {field}: {first.message}",
                details={"kind": kind, "field": field},
            )
