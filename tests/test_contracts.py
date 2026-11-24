"""Maintained contract regression suite covering audit findings R01-R20."""

from __future__ import annotations

import base64
import json
import secrets
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path
from threading import Barrier

import pytest

from ai_memory.config import MemoryConfig
from ai_memory.consolidation import ConsolidationResult
from ai_memory.core import MemoryCore
from ai_memory.frontmatter import render_markdown
from ai_memory.worker import BackgroundWorker

REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def setup(tmp_path):
    core = MemoryCore(MemoryConfig(data_root=tmp_path / "data", repo_root=REPO))
    owner, _ = core.access.create_token(label="audit-owner", allowed_scopes=["*"], can_forget=True)
    work, _ = core.access.create_token(label="audit-work", allowed_scopes=["shared", "work"])
    ctx = open_context(core, owner)
    yield core, owner, work, ctx
    core.close()


def open_context(core, client, scopes=None):
    payload = {"purpose": "isolated audit"}
    if scopes is not None:
        payload["scopes"] = scopes
    result = core.dispatch(client=client, operation="open_context", payload=payload)
    assert result["ok"], result
    return result["data"]["context_id"]


def call(core, client, ctx, operation, payload, key=None):
    return core.dispatch(
        client=client,
        context_id=ctx,
        operation=operation,
        payload=payload,
        idempotency_key=key or secrets.token_hex(12),
    )


def event(core, owner, ctx, scope="personal", content="Source statement for this audit."):
    session = call(
        core,
        owner,
        ctx,
        "start_session",
        {"source_application": "isolated-audit", "capture_coverage": "partial_events", "scope_id": scope},
    )["data"]["session_id"]
    result = call(
        core,
        owner,
        ctx,
        "append_events",
        {
            "session_id": session,
            "events": [{"source_event_id": secrets.token_hex(12), "speaker": "user", "content": content}],
        },
    )
    return result["data"]["accepted_ids"][0]


def memory(core, owner, ctx, scope="personal", **overrides):
    source = event(core, owner, ctx, scope)
    payload = {
        "type": "fact",
        "scope_id": scope,
        "title": "Audit memory",
        "body": "Original audit content.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    payload.update(overrides)
    result = call(core, owner, ctx, "create_memory", payload)
    assert result["ok"], result
    return result["data"]["record_id"], payload


def test_r01_existing_context_respects_reduced_current_credential_scopes(setup):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx)
    narrowed_identity = replace(owner, allowed_scopes=("work",))
    response = call(core, narrowed_identity, ctx, "read_page", {"record_id": record_id})
    assert not response["ok"]


def test_r02_cached_mutation_still_requires_valid_context(setup):
    core, owner, work, ctx = setup
    source = event(core, owner, ctx)
    payload = {
        "type": "fact",
        "scope_id": "personal",
        "title": "Cached result",
        "body": "Personal claim.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    first = call(core, owner, ctx, "create_memory", payload, "cached-audit")
    assert first["ok"]
    replay = call(core, owner, None, "create_memory", payload, "cached-audit")
    assert not replay["ok"]
    assert replay["error"]["code"] == "context_required"


def test_r03_profile_respects_its_personal_scope(setup):
    core, owner, work, ctx = setup
    (core.config.vault_dir / "PROFILE.md").write_text(
        "---\nscope_id: personal\n---\nPrivate personal profile marker.", encoding="utf-8"
    )
    response = core.dispatch(client=work, operation="open_context", payload={"purpose": "work only"})
    assert "Private personal profile marker." not in json.dumps(response)


def test_r04_private_relation_ids_are_absent_from_entire_response(setup):
    core, owner, work, ctx = setup
    private_id, _ = memory(core, owner, ctx)
    work_id, _ = memory(core, owner, ctx, "work", relations=[{"type": "related_to", "target_id": private_id}])
    response = call(core, work, open_context(core, work), "read_page", {"record_id": work_id})
    assert private_id not in json.dumps(response)


def test_r05_nonexistent_and_mismatched_quote_evidence_rejected(setup):
    core, owner, work, ctx = setup
    result = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "personal",
            "title": "Unsupported claim",
            "body": "No supporting event exists.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": "evt_nonexistent123", "quote": "Invented quotation"}],
        },
    )
    assert not result["ok"]

    real_evt = event(core, owner, ctx, "personal", "I prefer Python for scripting.")
    bad_quote = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "personal",
            "title": "Mismatched quote",
            "body": "Claim with fabricated quote.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": real_evt, "quote": "Completely different text"}],
        },
    )
    assert not bad_quote["ok"]


def test_r06_private_source_cannot_be_laundered_to_work(setup):
    core, owner, work, ctx = setup
    private_source = event(core, owner, ctx, "personal", "Private source text.")
    work_ctx = open_context(core, work)
    result = call(
        core,
        work,
        work_ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "work",
            "title": "Laundered source",
            "body": "Claim derived from inaccessible evidence.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": private_source, "quote": "Private source text."}],
        },
    )
    assert not result["ok"]


def test_r07_inference_cannot_be_promoted_by_update(setup):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx, origin="inference")
    result = call(
        core,
        owner,
        ctx,
        "update_memory",
        {
            "record_id": record_id,
            "expected_revision": 1,
            "changes": {"status": "current"},
            "reason": "Unsupported promotion",
        },
    )
    assert not result["ok"]
    assert core.index.get_record(record_id)["status"] == "uncertain"


def test_r08_concurrent_identical_mutations_have_one_effect(setup, monkeypatch):
    core, owner, work, ctx = setup
    source = event(core, owner, ctx)
    payload = {
        "type": "fact",
        "scope_id": "personal",
        "title": "Concurrent request",
        "body": "Only one record should exist.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    original = core.index.get_idempotency
    barrier = Barrier(2)

    def synchronized_read(client_id, key):
        previous = original(client_id, key)
        barrier.wait(timeout=10)
        return previous

    monkeypatch.setattr(core.index, "get_idempotency", synchronized_read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(call, core, owner, ctx, "create_memory", payload, "same-concurrent-key") for _ in range(2)
        ]
        responses = [future.result(timeout=20) for future in futures]
    assert all(response["ok"] for response in responses), responses
    assert len({response["data"]["record_id"] for response in responses}) == 1


def test_r09_retry_after_receipt_failure_does_not_duplicate(setup, monkeypatch):
    core, owner, work, ctx = setup
    source = event(core, owner, ctx)
    payload = {
        "type": "fact",
        "scope_id": "personal",
        "title": "Receipt recovery",
        "body": "One logical mutation.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    original = core.index.save_idempotency

    def fail_receipt(**kwargs):
        raise RuntimeError("Simulated interruption before receipt persistence")

    monkeypatch.setattr(core.index, "save_idempotency", fail_receipt)
    with pytest.raises(RuntimeError):
        call(core, owner, ctx, "create_memory", payload, "receipt-failure")
    monkeypatch.setattr(core.index, "save_idempotency", original)
    retry = call(core, owner, ctx, "create_memory", payload, "receipt-failure")
    assert retry["ok"]
    assert len(core.index.list_records(kind="memory")) == 1


def test_r10_manual_edit_between_prepare_and_apply_survives(setup, monkeypatch):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx)
    path = Path(core.index.get_record(record_id)["file_path"])
    original = core.journal._apply_targets

    def edit_during_prepared(targets, *, fail_midway=False):
        path.write_text(path.read_text(encoding="utf-8") + "\nIntervening manual edit marker.\n", encoding="utf-8")
        return original(targets, fail_midway=fail_midway)

    monkeypatch.setattr(core.journal, "_apply_targets", edit_during_prepared)
    resp = call(
        core,
        owner,
        ctx,
        "update_memory",
        {
            "record_id": record_id,
            "expected_revision": 1,
            "changes": {"body": "Automated replacement."},
            "reason": "Race probe",
        },
    )
    assert not resp["ok"] and resp["error"]["code"] == "revision_conflict"
    assert "Intervening manual edit marker." in path.read_text(encoding="utf-8")


def test_r11_previous_revision_content_is_retained_and_cleared_on_forget(setup):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx, body="Unique old revision marker for audit.")
    call(
        core,
        owner,
        ctx,
        "update_memory",
        {
            "record_id": record_id,
            "expected_revision": 1,
            "changes": {"body": "New revision body."},
            "reason": "Verify retained history",
        },
    )
    revisions = core.journal.list_revisions(record_id)
    assert len(revisions) == 1
    assert "Unique old revision marker for audit." in revisions[0]["body"]

    # Forgetting the record must also delete its revision history
    fgt = call(
        core,
        owner,
        ctx,
        "forget_record",
        {"record_id": record_id, "expected_revision": 2, "reason": "Delete history too", "confirm_forget": True},
    )
    assert fgt["ok"]
    assert core.journal.list_revisions(record_id) == []


def test_r12_startup_reconciliation_and_live_manual_edit_visibility(setup):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx)
    existing = core.index.get_record(record_id)
    meta = dict(existing["metadata"], id="mem_manualaudit01", title="Manually created")
    path = core.config.vault_dir / "Personal" / "Hobbies" / "manual.md"
    path.parent.mkdir(parents=True)
    path.write_text(render_markdown(meta, "# Manual note\nCreated outside API."), encoding="utf-8")
    restarted = MemoryCore(core.config)
    try:
        assert restarted.index.get_record("mem_manualaudit01") is not None
    finally:
        restarted.close()

    # Manual edit on an existing note is immediately visible via read_page
    rec_path = Path(core.index.get_record(record_id)["file_path"])
    rec_path.write_text(rec_path.read_text(encoding="utf-8") + "\nManual correction marker.\n", encoding="utf-8")
    response = call(core, owner, ctx, "read_page", {"record_id": record_id})
    assert "Manual correction marker." in response["data"]["body"]


def test_r13_forgetting_source_clears_derived_active_content(setup):
    core, owner, work, ctx = setup
    record_id, payload = memory(core, owner, ctx)
    source_id = payload["sources"][0]["record_id"]
    result = call(
        core,
        owner,
        ctx,
        "forget_record",
        {"record_id": source_id, "reason": "Forget related personal data", "confirm_forget": True},
    )
    assert result["ok"]
    assert record_id in result["data"]["derived_forgotten_ids"]
    assert core.index.get_record(record_id) is None


def test_r14_forgetting_document_removes_original_binary(setup):
    core, owner, work, ctx = setup
    response = call(
        core,
        owner,
        ctx,
        "ingest_document",
        {
            "scope_id": "personal",
            "original_filename": "audit.pdf",
            "media_type": "application/pdf",
            "content_base64": base64.b64encode(b"%PDF-1.4\nIsolated audit fixture").decode(),
        },
    )
    doc_id = response["data"]["document_id"]
    record = core.index.get_record(doc_id)
    original = core.config.records_dir / record["metadata"]["original_attachment_path"]
    assert original.exists()
    result = call(
        core,
        owner,
        ctx,
        "forget_record",
        {"record_id": doc_id, "expected_revision": 1, "reason": "Delete original", "confirm_forget": True},
    )
    assert result["ok"]
    assert not original.exists()


def test_r15_area_registration_rejects_existing_folder_collision(setup):
    core, owner, work, ctx = setup
    response = call(core, owner, ctx, "register_area", {"slug": "academics", "folder": "Personal"})
    assert not response["ok"]


def test_r16_search_does_not_truncate_before_authorization(setup):
    core, owner, work, ctx = setup
    for i in range(10):
        scope = "work" if i == 0 else "personal"
        meta = {
            "scope_id": scope,
            "title": f"Index fixture {i}",
            "type": "fact",
            "revision": 1,
            "updated_at": f"2026-11-{i+1:02d}T10:00:00Z",
        }
        core.index.upsert_record(
            record_id=f"mem_indexfixture{i}",
            kind="memory",
            meta=meta,
            body="Search fixture.",
            file_path=core.config.vault_dir / f"fixture-{i}.md",
            raw_hash="audit",
        )
    response = call(core, work, open_context(core, work), "search_memory", {"query": "", "limit": 1})
    assert response["data"]["total"] == 1


def test_r17_bootstrap_rules_and_events_readable_through_read_page(setup):
    core, owner, work, ctx = setup
    policy = call(core, owner, ctx, "read_page", {"record_id": "System/Rules/CREATE.md"})
    assert policy["ok"]
    assert policy["data"]["kind"] == "policy"
    assert "idempotency_key" in policy["data"]["body"]

    evt_id = event(core, owner, ctx, "personal", "Verifiable raw source text.")
    ev_read = call(core, owner, ctx, "read_page", {"record_id": evt_id})
    assert ev_read["ok"]
    assert "Verifiable raw source text." in ev_read["data"]["body"]

    # Work-only client cannot read personal raw event
    denied = call(core, work, open_context(core, work), "read_page", {"record_id": evt_id})
    assert not denied["ok"]
    assert denied["error"]["code"] == "not_found_or_unavailable"


def test_r18_r19_r20_worker_document_extraction_and_rejected_proposal_failure(setup):
    core, owner, work, ctx = setup

    # 1. Text document ingestion -> BackgroundWorker extracts and indexes content (R19)
    ing = call(
        core,
        owner,
        ctx,
        "ingest_document",
        {
            "scope_id": "personal",
            "original_filename": "notes.txt",
            "media_type": "text/plain",
            "content_base64": base64.b64encode(b"Quantum architecture meeting notes.").decode(),
        },
    )
    assert ing["ok"]
    doc_id = ing["data"]["document_id"]

    worker = BackgroundWorker(core)
    outcomes = worker.run_once()
    assert any(o.get("document_id") == doc_id and o.get("extraction_status") == "extracted" for o in outcomes)
    doc_page = call(core, owner, ctx, "read_page", {"record_id": doc_id})
    assert "Quantum architecture meeting notes." in doc_page["data"]["body"]

    # 2. Consolidation job with an invalid proposal MUST fail instead of silently completing (R20)
    class BadProposalProvider:
        def consolidate(self, **kwargs):
            return ConsolidationResult(
                proposals=[
                    {
                        "kind": "memory",
                        "type": "fact",
                        "title": "Hallucinated proposal",
                        "body": "Cites nonexistent event.",
                        "origin": "explicit_user_statement",
                        "sources": [{"record_id": "evt_does_not_exist"}],
                    }
                ]
            )

    core.provider = BadProposalProvider()
    ses = call(
        core,
        owner,
        ctx,
        "start_session",
        {"source_application": "pytest", "capture_coverage": "partial_events", "scope_id": "personal"},
    )["data"]["session_id"]
    call(
        core,
        owner,
        ctx,
        "end_session",
        {"session_id": ses},
    )
    consolidate_outcomes = worker.run_once()
    assert len(consolidate_outcomes) == 1
    assert consolidate_outcomes[0]["status"] == "failed"
    assert len(consolidate_outcomes[0]["rejected"]) == 1
