"""Replaceable consolidation provider interface and deterministic stub (Design Section 14)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from .ids import utc_now_iso


@dataclass(frozen=True)
class ConsolidationResult:
    proposals: list[dict[str, Any]]
    usage_metadata: dict[str, Any] = field(default_factory=dict)
    provider_metadata: dict[str, Any] = field(default_factory=dict)


class ConsolidationProvider(Protocol):
    """Replaceable backend contract: returns structured proposals, never raw file edits."""

    def consolidate(
        self,
        *,
        events: list[dict[str, Any]],
        relevant_memories: list[dict[str, Any]],
        schema_version: str,
        policy_constraints: dict[str, Any],
    ) -> ConsolidationResult: ...


class StubConsolidationProvider:
    """
    Deterministic, zero-network consolidation stub for v1 verification and offline safety.
    Replaceable by a Gemini API adapter once credentials, quota, and data-use terms are verified.
    """

    provider_name = "stub-deterministic"
    model_name = "stub-v1"

    def consolidate(
        self,
        *,
        events: list[dict[str, Any]],
        relevant_memories: list[dict[str, Any]],
        schema_version: str,
        policy_constraints: dict[str, Any],
    ) -> ConsolidationResult:
        proposals: list[dict[str, Any]] = []
        for evt in events:
            speaker = evt.get("speaker")
            text = (evt.get("content") or "").strip()
            if not text:
                continue
            lower = text.lower()
            # Respect epistemic discipline: temporary evaluation -> state, never lasting preference
            if speaker == "user" and ("trying" in lower or "evaluating" in lower or "this week" in lower):
                proposals.append(
                    {
                        "kind": "memory",
                        "type": "state",
                        "title": text[:80],
                        "body": f"Temporary activity noted from source event: {text}",
                        "origin": "explicit_user_statement",
                        "status": "current",
                        "sources": [{"record_id": evt["id"], "quote": text[:300]}],
                    }
                )
            elif speaker == "assistant" and ("suggest" in lower or "recommend" in lower):
                proposals.append(
                    {
                        "kind": "task",
                        "title": text[:80],
                        "body": f"Assistant suggestion captured as proposal: {text}",
                        "origin": "assistant_proposal",
                        "proposal": True,
                        "status": "inbox",
                        "sources": [{"record_id": evt["id"], "quote": text[:300]}],
                    }
                )

        return ConsolidationResult(
            proposals=proposals,
            usage_metadata={"input_events": len(events), "tokens_estimated": 0},
            provider_metadata={
                "provider": self.provider_name,
                "model": self.model_name,
                "schema_version": schema_version,
                "extracted_at": utc_now_iso(),
            },
        )
