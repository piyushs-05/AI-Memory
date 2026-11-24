"""Replaceable consolidation provider interface and deterministic stub (Design Section 14)."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from .ids import utc_now_iso


def _resolve_temp_window(text_lower: str, recorded_at: str | None, tz_name: str) -> tuple[str | None, str | None]:
    """Resolve explicit temporary windows ('today', 'this weekend', 'this week') into YYYY-MM-DD bounds."""
    if not recorded_at:
        return None, None
    try:
        dt = datetime.fromisoformat(recorded_at.replace("Z", "+00:00")).astimezone(ZoneInfo(tz_name))
    except Exception:
        return None, None

    d = dt.date()
    wd = d.weekday()  # Monday=0 .. Sunday=6
    if "weekend" in text_lower:
        if wd <= 5:
            sat = d + timedelta(days=(5 - wd))
            sun = sat + timedelta(days=1)
        else:
            sat = d
            sun = d
        return sat.isoformat(), sun.isoformat()
    if "this week" in text_lower:
        sun = d + timedelta(days=(6 - wd))
        return d.isoformat(), sun.isoformat()
    if "today" in text_lower:
        return d.isoformat(), d.isoformat()
    return d.isoformat(), None


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
        tz_name = str(policy_constraints.get("timezone") or "Asia/Kolkata")
        # Collect event IDs already covered by existing active memories to avoid duplicate states
        already_cited_event_ids: set[str] = set()
        for mem in relevant_memories:
            meta = mem.get("metadata") or {}
            for src in meta.get("sources") or []:
                if isinstance(src, dict) and src.get("record_id"):
                    already_cited_event_ids.add(str(src["record_id"]))

        proposals: list[dict[str, Any]] = []
        for evt in events:
            speaker = evt.get("speaker")
            text = (evt.get("content") or "").strip()
            if not text:
                continue
            lower = text.lower()
            # Respect epistemic discipline: temporary evaluation -> state with resolved expiry,
            # skipped if an active memory already cites this event.
            if speaker == "user" and ("trying" in lower or "evaluating" in lower or "this week" in lower or "weekend" in lower):
                if evt["id"] in already_cited_event_ids:
                    continue
                valid_from, valid_until = _resolve_temp_window(lower, evt.get("recorded_at"), tz_name)
                proposals.append(
                    {
                        "kind": "memory",
                        "type": "state",
                        "title": text[:80],
                        "body": f"Temporary activity noted from source event: {text}",
                        "origin": "explicit_user_statement",
                        "status": "current",
                        "valid_from": valid_from,
                        "valid_until": valid_until,
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


class GeminiConsolidationProvider:
    """
    Optional replaceable Gemini API consolidation provider (Design Section 14).
    Activated ONLY when explicitly configured with an API key outside the vault and
    scope external-processing approval. Returns structured proposals only, never raw edits.
    """

    provider_name = "gemini-api"

    def __init__(
        self,
        *,
        api_key: str,
        model: str = "gemini-2.5-flash",
        allowed_external_scopes: tuple[str, ...] = (),
        timeout_seconds: int = 30,
    ) -> None:
        self._api_key = api_key
        self.model_name = model
        self._allowed_external_scopes = allowed_external_scopes
        self._timeout = timeout_seconds

    def consolidate(
        self,
        *,
        events: list[dict[str, Any]],
        relevant_memories: list[dict[str, Any]],
        schema_version: str,
        policy_constraints: dict[str, Any],
    ) -> ConsolidationResult:
        import json
        import urllib.request

        scope_id = str(policy_constraints.get("scope_id") or "")
        if self._allowed_external_scopes and scope_id not in self._allowed_external_scopes:
            raise PermissionError(f"Scope '{scope_id}' is not approved for external Gemini processing")

        prompt = (
            "You are a strict memory consolidation extractor. Given source events and existing memories, "
            "return ONLY valid JSON with key 'proposals' (array of memory or task proposals). "
            "Rules: cite exact source record_id and verbatim quote; temporary activity is type='state', "
            "not 'preference'; assistant suggestions are kind='task', status='inbox', proposal=true; "
            "inferences must have origin='inference' and status='uncertain'.\n"
            f"Events: {json.dumps(events, ensure_ascii=False)}\n"
            f"Existing memories: {json.dumps([{'id': m['id'], 'title': m['title'], 'type': m['record_type'], 'status': m['status']} for m in relevant_memories], ensure_ascii=False)}"
        )
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{self.model_name}:generateContent?key={self._api_key}"
        body = json.dumps(
            {
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"responseMimeType": "application/json", "temperature": 0.1},
            }
        ).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=self._timeout) as resp:
            raw = json.loads(resp.read().decode("utf-8"))
        text = raw["candidates"][0]["content"]["parts"][0]["text"]
        parsed = json.loads(text)
        return ConsolidationResult(
            proposals=list(parsed.get("proposals") or []),
            usage_metadata=dict(raw.get("usageMetadata") or {}),
            provider_metadata={
                "provider": self.provider_name,
                "model": self.model_name,
                "schema_version": schema_version,
                "extracted_at": utc_now_iso(),
            },
        )
