"""Standardized error codes and exception type (Design Section 9.3)."""

from __future__ import annotations

from typing import Any

ERROR_CODES = frozenset(
    {
        "authentication_required",
        "context_required",
        "context_expired",
        "policy_refresh_required",
        "not_found_or_unavailable",
        "validation_failed",
        "revision_conflict",
        "idempotency_conflict",
        "source_conflict",
        "temporarily_unavailable",
        "processing_failed",
    }
)


class MemoryServiceError(Exception):
    """Domain error mapped directly to the logical operation error envelope."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        if code not in ERROR_CODES:
            raise ValueError(f"Unsupported error code: {code}")
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details
        self.http_status = http_status or _DEFAULT_HTTP_STATUS[code]

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
        }
        if self.details is not None:
            payload["details"] = self.details
        return payload


_DEFAULT_HTTP_STATUS: dict[str, int] = {
    "authentication_required": 401,
    "context_required": 400,
    "context_expired": 401,
    "policy_refresh_required": 409,
    "not_found_or_unavailable": 404,
    "validation_failed": 400,
    "revision_conflict": 409,
    "idempotency_conflict": 409,
    "source_conflict": 409,
    "temporarily_unavailable": 503,
    "processing_failed": 500,
}
