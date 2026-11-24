"""Verification suite covering Design Section 16 acceptance scenarios."""

from __future__ import annotations

import base64
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from ai_memory.config import MemoryConfig
from ai_memory.core import MemoryCore
from ai_memory.http_server import make_http_server
from ai_memory.journal import FileMutationOp


@pytest.fixture()
def env(tmp_path: Path):
    repo_root = Path(__file__).resolve().parents[1]
    config = MemoryConfig(data_root=tmp_path / "memory-home", repo_root=repo_root)
    core = MemoryCore(config)
    owner, owner_token = core.access.create_token(
        label="owner", allowed_scopes=["shared", "work", "personal"], can_write=True, can_forget=True
    )
    work_only, work_token = core.access.create_token(
        label="work-agent", allowed_scopes=["shared", "work"], can_write=True, can_forget=False
    )
    yield {
        "config": config,
        "core": core,
        "owner": owner,
        "owner_token": owner_token,
        "work_only": work_only,
        "work_token": work_token,
    }
    core.close()


def _open(core: MemoryCore, client, scopes=None):
    resp = core.dispatch(
        client=client,
        operation="open_context",
        payload={"purpose": "test", **({"scopes": scopes} if scopes else {})},
    )
    assert resp["ok"], resp
    return resp["data"]["context_id"]


def _capture_event(core: MemoryCore, client, ctx: str, scope_id: str, content: str, key_prefix: str) -> str:
    ses = core.dispatch(
        client=client,
        operation="start_session",
        context_id=ctx,
        idempotency_key=f"{key_prefix}-ses",
        payload={"source_application": "pytest", "capture_coverage": "partial_events", "scope_id": scope_id},
    )
    assert ses["ok"], ses
    ev = core.dispatch(
        client=client,
        operation="append_events",
        context_id=ctx,
        idempotency_key=f"{key_prefix}-evt",
        payload={"session_id": ses["data"]["session_id"], "events": [{"speaker": "user", "content": content}]},
    )
    assert ev["ok"], ev
    return ev["data"]["accepted_ids"][0]


def test_idempotency_and_capture_deduplication(env):
    core, owner = env["core"], env["owner"]
    ctx = _open(core, owner)

    s1 = core.dispatch(
        client=owner,
        operation="start_session",
        context_id=ctx,
        idempotency_key="start-1",
        payload={
            "source_application": "pytest-harness",
            "capture_coverage": "full_accessible_transcript",
            "scope_id": "personal",
        },
    )
    assert s1["ok"]
    # Exact retry returns identical response without duplicating session
    s1_retry = core.dispatch(
        client=owner,
        operation="start_session",
        context_id=ctx,
        idempotency_key="start-1",
        payload={
            "source_application": "pytest-harness",
            "capture_coverage": "full_accessible_transcript",
            "scope_id": "personal",
        },
    )
    assert s1_retry["data"]["session_id"] == s1["data"]["session_id"]

    # Same key with different payload -> idempotency_conflict
    conflict = core.dispatch(
        client=owner,
        operation="start_session",
        context_id=ctx,
        idempotency_key="start-1",
        payload={
            "source_application": "different-app",
            "capture_coverage": "partial_events",
            "scope_id": "personal",
        },
    )
    assert not conflict["ok"]
    assert conflict["error"]["code"] == "idempotency_conflict"

    ses_id = s1["data"]["session_id"]
    b1 = core.dispatch(
        client=owner,
        operation="append_events",
        context_id=ctx,
        idempotency_key="batch-1",
        payload={
            "session_id": ses_id,
            "events": [
                {"source_event_id": "up-1", "speaker": "user", "content": "I am trying Rust this week."},
            ],
        },
    )
    assert b1["ok"] and len(b1["data"]["accepted_ids"]) == 1

    # Duplicate source_event_id with same content -> duplicate_ids, not re-created
    b2 = core.dispatch(
        client=owner,
        operation="append_events",
        context_id=ctx,
        idempotency_key="batch-2",
        payload={
            "session_id": ses_id,
            "events": [
                {"source_event_id": "up-1", "speaker": "user", "content": "I am trying Rust this week."},
            ],
        },
    )
    assert b2["ok"]
    assert b2["data"]["accepted_ids"] == []
    assert b2["data"]["duplicate_ids"] == b1["data"]["accepted_ids"]

    # Same source_event_id with DIFFERENT content -> source_conflict
    b3 = core.dispatch(
        client=owner,
        operation="append_events",
        context_id=ctx,
        idempotency_key="batch-3",
        payload={
            "session_id": ses_id,
            "events": [
                {"source_event_id": "up-1", "speaker": "user", "content": "Altered content"},
            ],
        },
    )
    assert not b3["ok"]
    assert b3["error"]["code"] == "source_conflict"


def test_epistemic_rules_temporary_state_and_assistant_proposal(env):
    core, owner = env["core"], env["owner"]
    ctx = _open(core, owner)

    ses = core.dispatch(
        client=owner,
        operation="start_session",
        context_id=ctx,
        idempotency_key="s-epi",
        payload={"source_application": "cli", "capture_coverage": "partial_events", "scope_id": "personal"},
    )["data"]["session_id"]

    evts = core.dispatch(
        client=owner,
        operation="append_events",
        context_id=ctx,
        idempotency_key="e-epi",
        payload={
            "session_id": ses,
            "events": [
                {"speaker": "user", "content": "I am trying Rust this week."},
                {"speaker": "assistant", "content": "I suggest trying Cargo workspaces next."},
            ],
        },
    )["data"]["accepted_ids"]

    core.dispatch(
        client=owner,
        operation="end_session",
        context_id=ctx,
        idempotency_key="end-epi",
        payload={"session_id": ses},
    )
    ctx_row = core.index.get_context(ctx)
    results = core.run_pending_consolidation(client=owner, ctx=ctx_row)
    assert len(results) == 1
    created = results[0]["created_ids"]
    assert len(created) == 2

    recs = [core.index.get_record(rid) for rid in created]
    mem = next(r for r in recs if r["kind"] == "memory")
    task = next(r for r in recs if r["kind"] == "task")
    # Temporary Rust evaluation is stored as 'state', NOT 'preference'
    assert mem["record_type"] == "state"
    # Assistant suggestion enters 'inbox' as a proposal
    assert task["status"] == "inbox"
    assert task["metadata"]["proposal"] is True

    # Inference origin cannot be promoted directly to 'current'
    inf = core.dispatch(
        client=owner,
        operation="create_memory",
        context_id=ctx,
        idempotency_key="inf-1",
        payload={
            "type": "fact",
            "scope_id": "personal",
            "title": "Possible interest in systems programming",
            "body": "Inferred from Rust evaluation.",
            "origin": "inference",
            "status": "current",
            "sources": [{"record_id": evts[0]}],
        },
    )
    assert inf["ok"]
    assert inf["data"]["status"] == "uncertain"


def test_cross_scope_isolation_and_no_existence_leak(env):
    core, owner, work_only = env["core"], env["owner"], env["work_only"]
    owner_ctx = _open(core, owner)
    work_ctx = _open(core, work_only)

    # Create a personal memory and a work memory linking to it
    p_evt = _capture_event(core, owner, owner_ctx, "personal/life", "Doctor visit note.", "p-evt")
    w_evt = _capture_event(core, owner, owner_ctx, "work/projects", "Selected lightweight gateway.", "w-evt")

    p_mem = core.dispatch(
        client=owner,
        operation="create_memory",
        context_id=owner_ctx,
        idempotency_key="p-mem",
        payload={
            "type": "fact",
            "scope_id": "personal/life",
            "title": "Private Medical Appointment",
            "body": "Secret personal note about doctor visit.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": p_evt}],
        },
    )["data"]["record_id"]

    w_mem = core.dispatch(
        client=owner,
        operation="create_memory",
        context_id=owner_ctx,
        idempotency_key="w-mem",
        payload={
            "type": "decision",
            "scope_id": "work/projects",
            "title": "API Gateway Choice",
            "body": "Selected lightweight gateway.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": w_evt}],
            "relations": [{"type": "related_to", "target_id": p_mem}],
        },
    )["data"]["record_id"]

    # Work-only client cannot search or read personal memory
    search = core.dispatch(
        client=work_only,
        operation="search_memory",
        context_id=work_ctx,
        payload={"query": "Medical"},
    )
    assert search["ok"] and search["data"]["total"] == 0

    read_priv = core.dispatch(
        client=work_only,
        operation="read_page",
        context_id=work_ctx,
        payload={"record_id": p_mem},
    )
    assert not read_priv["ok"]
    assert read_priv["error"]["code"] == "not_found_or_unavailable"

    # Reading the work memory MUST NOT leak the linked personal memory's title or ID
    read_work = core.dispatch(
        client=work_only,
        operation="read_page",
        context_id=work_ctx,
        payload={"record_id": w_mem},
    )
    assert read_work["ok"]
    assert read_work["data"]["related"] == []


def test_manual_obsidian_edit_preserved_and_rename_keeps_identity(env):
    core, owner, config = env["core"], env["owner"], env["config"]
    ctx = _open(core, owner)

    obs_evt = _capture_event(core, owner, ctx, "personal", "Original content.", "obs-evt")
    mem_id = core.dispatch(
        client=owner,
        operation="create_memory",
        context_id=ctx,
        idempotency_key="m-obs",
        payload={
            "type": "fact",
            "scope_id": "personal",
            "title": "Editor Note",
            "body": "Original content.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": obs_evt}],
        },
    )["data"]["record_id"]

    rec = core.index.get_record(mem_id)
    fpath = Path(rec["file_path"])
    # Simulate manual Obsidian edit on disk
    fpath.write_text(fpath.read_text(encoding="utf-8") + "\nManual user correction.\n", encoding="utf-8")

    # Automated update must fail with revision_conflict instead of clobbering manual edit
    upd = core.dispatch(
        client=owner,
        operation="update_memory",
        context_id=ctx,
        idempotency_key="m-obs-upd",
        payload={
            "record_id": mem_id,
            "expected_revision": 1,
            "changes": {"body": "Automated overwrite attempt."},
            "reason": "Stale automated update",
        },
    )
    assert not upd["ok"]
    assert upd["error"]["code"] == "revision_conflict"
    assert "Manual user correction." in fpath.read_text(encoding="utf-8")

    # Rename file in Obsidian and rebuild index -> identity preserved!
    renamed = fpath.with_name("Renamed-Manually-In-Obsidian.md")
    fpath.rename(renamed)
    core.index.rebuild_from_vault(config.vault_dir)
    reindexed = core.index.get_record(mem_id)
    assert reindexed is not None
    assert Path(reindexed["file_path"]) == renamed
    assert "Manual user correction." in reindexed["body"]


def test_task_lifecycle_cycles_and_derived_views(env):
    core, owner, config = env["core"], env["owner"], env["config"]
    ctx = _open(core, owner)

    t1_evt = _capture_event(core, owner, ctx, "work", "Ship v1 core.", "t1-evt")
    t2_evt = _capture_event(core, owner, ctx, "work", "Write docs.", "t2-evt")
    t1 = core.dispatch(
        client=owner,
        operation="create_task",
        context_id=ctx,
        idempotency_key="t1",
        payload={
            "scope_id": "work",
            "title": "Ship v1 core",
            "status": "next",
            "due_date": "2020-01-01",
            "sources": [{"record_id": t1_evt}],
        },
    )["data"]["record_id"]

    t2 = core.dispatch(
        client=owner,
        operation="create_task",
        context_id=ctx,
        idempotency_key="t2",
        payload={
            "scope_id": "work",
            "title": "Write docs",
            "status": "next",
            "depends_on": [t1],
            "sources": [{"record_id": t2_evt}],
        },
    )["data"]["record_id"]

    # Dependency cycle t1 -> t2 -> t1 must be rejected
    cyc = core.dispatch(
        client=owner,
        operation="update_task",
        context_id=ctx,
        idempotency_key="t1-cyc",
        payload={
            "record_id": t1,
            "expected_revision": 1,
            "changes": {"depends_on": [t2]},
        },
    )
    assert not cyc["ok"] and cyc["error"]["code"] == "validation_failed"

    # Transition next -> in_progress -> done (requires evidence or explicit instruction)
    r2 = core.dispatch(
        client=owner,
        operation="update_task",
        context_id=ctx,
        idempotency_key="t1-ip",
        payload={"record_id": t1, "expected_revision": 1, "changes": {"status": "in_progress"}},
    )
    assert r2["ok"]

    no_ev = core.dispatch(
        client=owner,
        operation="update_task",
        context_id=ctx,
        idempotency_key="t1-done-noev",
        payload={"record_id": t1, "expected_revision": 2, "changes": {"status": "done"}},
    )
    assert not no_ev["ok"]

    done = core.dispatch(
        client=owner,
        operation="update_task",
        context_id=ctx,
        idempotency_key="t1-done",
        payload={
            "record_id": t1,
            "expected_revision": 2,
            "changes": {"status": "done"},
            "explicit_user_instruction": True,
        },
    )
    assert done["ok"]

    # Verify TODAY.md and Work/TASKS.md were generated
    assert (config.vault_dir / "TODAY.md").is_file()
    work_tasks_md = (config.vault_dir / "Work" / "TASKS.md").read_text(encoding="utf-8")
    assert "Ship v1 core" in work_tasks_md
    assert "Write docs" in work_tasks_md


def test_crash_recovery_and_forget_tombstone(env):
    core, owner, config = env["core"], env["owner"], env["config"]
    ctx = _open(core, owner)

    # Simulate a crash right after journal enters Prepared
    target = config.vault_dir / "Personal" / "Memories" / "crashed--mem_crash01.md"
    content = "---\nschema_version: '1.0'\nid: mem_crash01\ntype: fact\nscope_id: personal\ntitle: Crash Test\nrevision: 1\nstatus: current\norigin: explicit_user_statement\nrecorded_at: '2026-11-24T10:00:00Z'\nupdated_at: '2026-11-24T10:00:00Z'\nsources:\n  - record_id: evt_c1\n---\n# Crash Test\n\nRecovered cleanly.\n"
    with pytest.raises(RuntimeError):
        core.journal.execute(
            operation="create_memory",
            ops=[FileMutationOp(path=target, action="write", after_content=content)],
            fail_after_state="Prepared",
        )
    assert not target.exists()

    # Restarting core recovers the Prepared mutation deterministically
    recovered_core = MemoryCore(config)
    try:
        assert target.exists()
        assert recovered_core.index.get_record("mem_crash01") is not None

        # Forget an event and verify tombstone prevents ordinary reimport
        ses = recovered_core.dispatch(
            client=owner,
            operation="start_session",
            context_id=ctx,
            idempotency_key="s-fgt",
            payload={"source_application": "cli", "capture_coverage": "partial_events", "scope_id": "personal"},
        )["data"]["session_id"]
        evt_id = recovered_core.dispatch(
            client=owner,
            operation="append_events",
            context_id=ctx,
            idempotency_key="e-fgt",
            payload={
                "session_id": ses,
                "events": [{"source_event_id": "secret-up-1", "speaker": "user", "content": "Forget me."}],
            },
        )["data"]["accepted_ids"][0]

        fgt = recovered_core.dispatch(
            client=owner,
            operation="forget_record",
            context_id=ctx,
            idempotency_key="do-fgt",
            payload={"record_id": evt_id, "reason": "User requested deletion", "confirm_forget": True},
        )
        assert fgt["ok"] and fgt["data"]["tombstone_recorded"] is True

        reimport = recovered_core.dispatch(
            client=owner,
            operation="append_events",
            context_id=ctx,
            idempotency_key="e-reimport",
            payload={
                "session_id": ses,
                "events": [{"source_event_id": "secret-up-1", "speaker": "user", "content": "Forget me."}],
            },
        )
        assert reimport["ok"]
        assert reimport["data"]["accepted_ids"] == []
        assert "secret-up-1" in reimport["data"]["skipped_tombstoned"]
    finally:
        recovered_core.close()


def test_http_and_mcp_endpoints_end_to_end(env):
    core, owner_token = env["core"], env["owner_token"]
    server = make_http_server(core, host="127.0.0.1", port=0)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # 1. /health (no auth required, operational metadata only)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/health") as r:
            health = json.loads(r.read().decode("utf-8"))
            assert health["status"] == "ok"

        # 2. /v1/ops without token -> 401
        req_unauth = urllib.request.Request(
            f"http://127.0.0.1:{port}/v1/ops",
            data=json.dumps({"operation": "open_context", "input": {"purpose": "x"}}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            urllib.request.urlopen(req_unauth)
        assert exc_info.value.code == 401

        # 3. /mcp initialize + tools/list + tools/call open_context
        def mcp_post(payload: dict) -> dict:
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/mcp",
                data=json.dumps(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {owner_token}",
                },
                method="POST",
            )
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode("utf-8"))

        init_resp = mcp_post({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        assert init_resp["result"]["serverInfo"]["name"] == "ai-memory"

        tools_resp = mcp_post({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
        tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
        assert {"open_context", "search_memory", "create_memory", "create_task", "ingest_document"} <= tool_names

        call_resp = mcp_post(
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "open_context", "arguments": {"purpose": "mcp-test"}},
            }
        )
        assert not call_resp["result"]["isError"]
        assert call_resp["result"]["structuredContent"]["data"]["context_id"].startswith("ctx_")
    finally:
        server.shutdown()
        server.server_close()


def test_dynamic_areas_nested_scopes_and_custom_folders(env):
    core, config, work_only = env["core"], env["config"], env["work_only"]
    wildcard_owner, _ = core.access.create_token(
        label="wildcard-owner", allowed_scopes=["*"], can_write=True, can_forget=True
    )
    owner_ctx = _open(core, wildcard_owner)
    work_ctx = _open(core, work_only)

    # Restricted work-only client cannot register an unauthorized top-level area
    denied = core.dispatch(
        client=work_only,
        operation="register_area",
        context_id=work_ctx,
        idempotency_key="reg-denied",
        payload={"slug": "academics"},
    )
    assert not denied["ok"] and denied["error"]["code"] == "validation_failed"

    # Owner dynamically adds a brand-new top-level area ("academics") with custom subfolders
    reg = core.dispatch(
        client=wildcard_owner,
        operation="register_area",
        context_id=owner_ctx,
        idempotency_key="reg-acad",
        payload={
            "slug": "academics",
            "folder": "Academics",
            "description": "Courses, labs, and research.",
            "subfolders": ["Courses", "Research", "Tasks", "Memories"],
        },
    )
    assert reg["ok"], reg
    assert (config.vault_dir / "Academics" / "INDEX.md").is_file()
    assert (config.vault_dir / "Academics" / "Courses").is_dir()
    assert (config.vault_dir / "Academics" / "TASKS.md").is_file()

    # Nested sub-scope automatically creates nested folders inside Academics/
    acad_evt1 = _capture_event(core, wildcard_owner, owner_ctx, "academics/semester-6/os", "Multi-level page tables.", "acad-e1")
    nested_mem = core.dispatch(
        client=wildcard_owner,
        operation="create_memory",
        context_id=owner_ctx,
        idempotency_key="m-acad-nested",
        payload={
            "type": "lesson",
            "scope_id": "academics/semester-6/os",
            "title": "Page Table Walk",
            "body": "Multi-level page tables save memory for sparse address spaces.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": acad_evt1}],
        },
    )
    assert nested_mem["ok"], nested_mem
    rec = core.index.get_record(nested_mem["data"]["record_id"])
    rel = Path(rec["file_path"]).relative_to(config.vault_dir)
    assert rel.parts[:4] == ("Academics", "Semester-6", "Os", "Memories")

    # Custom folder subpath inside Personal/ works dynamically
    photo_evt = _capture_event(core, wildcard_owner, owner_ctx, "personal", "35mm prime lens notes.", "photo-e1")
    custom_mem = core.dispatch(
        client=wildcard_owner,
        operation="create_memory",
        context_id=owner_ctx,
        idempotency_key="m-custom-folder",
        payload={
            "type": "fact",
            "scope_id": "personal",
            "folder": "Hobbies/Photography/Lenses",
            "title": "35mm Prime Notes",
            "body": "Great for street photography.",
            "origin": "explicit_user_statement",
            "sources": [{"record_id": photo_evt}],
        },
    )
    assert custom_mem["ok"], custom_mem
    c_rec = core.index.get_record(custom_mem["data"]["record_id"])
    c_rel = Path(c_rec["file_path"]).relative_to(config.vault_dir)
    assert c_rel.parts[:4] == ("Personal", "Hobbies", "Photography", "Lenses")

    # Task under new area populates Academics/TASKS.md dynamically
    acad_evt2 = _capture_event(core, wildcard_owner, owner_ctx, "academics/semester-6", "Submit OS lab report.", "acad-e2")
    acad_task = core.dispatch(
        client=wildcard_owner,
        operation="create_task",
        context_id=owner_ctx,
        idempotency_key="t-acad-1",
        payload={
            "scope_id": "academics/semester-6",
            "title": "Submit OS lab report",
            "status": "next",
            "sources": [{"record_id": acad_evt2}],
        },
    )
    assert acad_task["ok"]
    assert "Submit OS lab report" in (config.vault_dir / "Academics" / "TASKS.md").read_text(encoding="utf-8")

    # Work-only client's list_areas does not expose Academics
    w_areas = core.dispatch(client=work_only, operation="list_areas", context_id=work_ctx)
    assert w_areas["ok"]
    assert "academics" not in w_areas["data"]["areas"]
    assert "work" in w_areas["data"]["areas"]

