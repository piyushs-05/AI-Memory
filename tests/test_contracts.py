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


# -- Follow-up boundary regressions (F01-F08) ---------------------------------


def test_f01_linux_mutation_and_cross_process_lock_no_deadlock(tmp_path):
    """F01: Nested mutation_boundary + execute must not deadlock on Linux, while still locking across processes."""
    import os
    import subprocess
    import sys

    if os.name == "nt":
        pytest.skip("Linux/POSIX flock regression")

    data_root = tmp_path / "linux-lock-data"
    script = (
        "from pathlib import Path; "
        "from ai_memory.config import MemoryConfig; "
        "from ai_memory.core import MemoryCore; "
        f"core = MemoryCore(MemoryConfig(data_root=Path({str(data_root)!r}), repo_root=Path({str(REPO)!r}))); "
        "owner, _ = core.access.create_token(label='t', allowed_scopes=['personal']); "
        "ctx = core.dispatch(client=owner, operation='open_context', payload={'purpose': 'lock test'})['data']['context_id']; "
        "res = core.dispatch(client=owner, context_id=ctx, operation='start_session', idempotency_key='k1', "
        "payload={'source_application': 'linux-test', 'capture_coverage': 'partial_events', 'scope_id': 'personal'}); "
        "assert res['ok'], res; "
        "core.close(); print('OK')"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    assert proc.returncode == 0 and "OK" in proc.stdout, proc.stderr

    # Cross-process exclusivity: while parent holds mutation_boundary, a child nonblocking flock MUST fail
    core = MemoryCore(MemoryConfig(data_root=data_root, repo_root=REPO))
    try:
        with core.journal.mutation_boundary():
            lock_probe = (
                "import fcntl\n"
                f"f = open({str(core.journal._lock_file_path)!r}, 'a+b')\n"
                "try:\n"
                "    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
                "    print('ACQUIRED')\n"
                "except BlockingIOError:\n"
                "    print('BLOCKED')\n"
                "finally:\n"
                "    f.close()\n"
            )
            probe = subprocess.run(
                [sys.executable, "-c", lock_probe],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            assert probe.returncode == 0 and "BLOCKED" in probe.stdout, probe.stdout + probe.stderr
    finally:
        core.close()


def test_f02_policy_reader_cannot_escape_policy_directory(setup):
    core, owner, work, ctx = setup
    (core.config.vault_dir / "PROFILE.md").write_text(
        "---\nscope_id: personal\n---\nPrivate profile secret marker.", encoding="utf-8"
    )
    work_ctx = open_context(core, work)
    for traversal in (
        "System/Rules/../../PROFILE.md",
        "System/Templates/../../SUMMARY.md",
        "System\\Rules\\..\\..\\PROFILE.md",
    ):
        resp = call(core, work, work_ctx, "read_page", {"record_id": traversal})
        assert not resp["ok"]
        assert "Private profile secret marker." not in json.dumps(resp)


def test_f03_underscore_in_scope_is_not_a_sql_wildcard(setup):
    core, owner, work, ctx = setup
    evt_xa = event(core, owner, ctx, "work/teamxa/private", "Secret teamxa content.")
    mem_xa = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "work/teamxa/private",
            "title": "TeamXA Private",
            "body": "UniqueTeamXAMarker content.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": evt_xa}],
        },
    )
    assert mem_xa["ok"]

    evt_a = event(core, owner, ctx, "work/team_a/notes", "Allowed team_a content.")
    mem_a = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "work/team_a/notes",
            "title": "Team_A Allowed",
            "body": "AllowedTeamAMarker content.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": evt_a}],
        },
    )
    assert mem_a["ok"]

    team_a_client, _ = core.access.create_token(label="team-a", allowed_scopes=["work/team_a"])
    team_a_ctx = open_context(core, team_a_client)

    # FTS search must NOT leak work/teamxa/private to work/team_a
    leak_fts = call(core, team_a_client, team_a_ctx, "search_memory", {"query": "UniqueTeamXAMarker"})
    assert leak_fts["ok"] and leak_fts["data"]["total"] == 0

    # Empty-query search must return only team_a, not teamxa
    empty_res = call(core, team_a_client, team_a_ctx, "search_memory", {"query": "", "limit": 20})
    assert empty_res["ok"]
    returned_scopes = {r["scope_id"] for r in empty_res["data"]["results"]}
    assert "work/teamxa/private" not in returned_scopes
    assert "work/team_a/notes" in returned_scopes


def test_f04_crash_before_journal_response_does_not_duplicate(setup, monkeypatch):
    core, owner, work, ctx = setup
    source = event(core, owner, ctx)
    payload = {
        "type": "fact",
        "scope_id": "personal",
        "title": "Crash before record_response",
        "body": "Must commit once.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    original = core.journal.record_response

    def crash_before_response(mut_id, response):
        raise RuntimeError("Simulated crash before journal.record_response")

    monkeypatch.setattr(core.journal, "record_response", crash_before_response)
    with pytest.raises(RuntimeError):
        call(core, owner, ctx, "create_memory", payload, "crash-before-resp-key")

    monkeypatch.setattr(core.journal, "record_response", original)
    retry = call(core, owner, ctx, "create_memory", payload, "crash-before-resp-key")
    assert retry["ok"]
    assert len(core.index.list_records(kind="memory")) == 1


def test_f05_restored_tombstoned_note_stays_forgotten(setup):
    core, owner, work, ctx = setup
    record_id, _ = memory(core, owner, ctx)
    rec = core.index.get_record(record_id)
    note_path = Path(rec["file_path"])
    saved_bytes = note_path.read_bytes()

    fgt = call(
        core,
        owner,
        ctx,
        "forget_record",
        {"record_id": record_id, "expected_revision": 1, "reason": "Forget note", "confirm_forget": True},
    )
    assert fgt["ok"]

    # Simulate accidental restore via Obsidian undo / sync
    note_path.write_bytes(saved_bytes)
    core.index.reconcile_vault(core.config.vault_dir)
    assert core.index.get_record(record_id) is None

    read_resp = call(core, owner, ctx, "read_page", {"record_id": record_id})
    assert not read_resp["ok"]
    assert read_resp["error"]["code"] == "not_found_or_unavailable"


def test_f06_multisource_forgetting_removes_affected_content_and_history(setup):
    core, owner, work, ctx = setup
    src_a = event(core, owner, ctx, "personal", "PrivateSensitiveMarker123 from source A.")
    src_b = event(core, owner, ctx, "personal", "Unrelated fact from source B.")

    mem_resp = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "fact",
            "scope_id": "personal",
            "title": "Combined note",
            "body": "Combines PrivateSensitiveMarker123 with source B.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": src_a}, {"record_id": src_b}],
        },
    )
    assert mem_resp["ok"]
    mem_id = mem_resp["data"]["record_id"]

    # Update title once so a historical revision is retained
    upd = call(
        core,
        owner,
        ctx,
        "update_memory",
        {
            "record_id": mem_id,
            "expected_revision": 1,
            "changes": {"title": "Combined note v2"},
            "reason": "Rename title",
        },
    )
    assert upd["ok"]
    assert len(core.journal.list_revisions(mem_id)) == 1

    # Forget source A -> derived mixed-source record AND its revision history must be removed
    fgt = call(
        core,
        owner,
        ctx,
        "forget_record",
        {"record_id": src_a, "reason": "Remove private marker", "confirm_forget": True},
    )
    assert fgt["ok"]
    assert core.index.get_record(mem_id) is None
    assert core.journal.list_revisions(mem_id) == []

    # Verify PrivateSensitiveMarker123 does not survive in active vault or revision history
    for p in core.config.vault_dir.rglob("*.md"):
        assert "PrivateSensitiveMarker123" not in p.read_text(encoding="utf-8")


def test_f07_assistant_proposal_is_not_accepted_user_decision(setup):
    core, owner, work, ctx = setup
    ses = call(
        core,
        owner,
        ctx,
        "start_session",
        {"source_application": "audit", "capture_coverage": "partial_events", "scope_id": "personal"},
    )["data"]["session_id"]
    asst_evt = call(
        core,
        owner,
        ctx,
        "append_events",
        {
            "session_id": ses,
            "events": [{"speaker": "assistant", "content": "I suggest switching all projects to Rust."}],
        },
    )["data"]["accepted_ids"][0]

    # Attempting to label an assistant-only suggestion as explicit_user_statement decision must be rejected
    bad = call(
        core,
        owner,
        ctx,
        "create_memory",
        {
            "type": "decision",
            "scope_id": "personal",
            "title": "Switch all projects to Rust",
            "body": "User accepted switching all projects to Rust.",
            "origin": "explicit_user_statement",
            "status": "current",
            "sources": [{"record_id": asst_evt, "quote": "I suggest switching all projects to Rust."}],
        },
    )
    assert not bad["ok"] and bad["error"]["code"] == "validation_failed"


def test_f08_cached_response_obeys_narrower_valid_context(setup):
    core, owner, work, ctx = setup
    source = event(core, owner, ctx, "personal", "Personal note for F08.")
    payload = {
        "type": "fact",
        "scope_id": "personal",
        "title": "Personal cached note",
        "body": "Secret personal body.",
        "origin": "explicit_user_statement",
        "sources": [{"record_id": source}],
    }
    first = call(core, owner, ctx, "create_memory", payload, "f08-shared-key")
    assert first["ok"]
    personal_id = first["data"]["record_id"]

    # Same client opens a narrower work-only context and replays the same key/payload
    work_ctx = open_context(core, owner, scopes=["work"])
    replay = call(core, owner, work_ctx, "create_memory", payload, "f08-shared-key")
    assert not replay["ok"]
    assert personal_id not in json.dumps(replay)
    # Must NOT have re-executed or created a second record
    assert len(core.index.list_records(kind="memory")) == 1

