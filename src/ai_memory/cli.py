"""Command-line interface for administering and serving the Portable AI Memory system."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .bootstrap import init_memory_home
from .config import MemoryConfig
from .core import MemoryCore
from .http_server import make_http_server
from .mcp_server import serve_stdio
from .worker import BackgroundWorker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai-memory", description="Portable AI Memory Service CLI")
    parser.add_argument("--data-root", type=Path, default=None, help="Override AI_MEMORY_HOME directory")
    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-vault", help="Initialize memory-home directories, schemas, and bootstrap files")
    p_init.add_argument("--overwrite-policy", action="store_true", help="Refresh protected policy files and schemas")

    p_tok = sub.add_parser("create-token", help="Create a scoped bearer token (printed once)")
    p_tok.add_argument("--label", required=True, help="Human-readable client label (e.g. windows-owner)")
    p_tok.add_argument("--scopes", required=True, help="Comma-separated scopes (e.g. shared,work,personal)")
    p_tok.add_argument("--read-only", action="store_true", help="Disallow write mutations")
    p_tok.add_argument("--allow-forget", action="store_true", help="Permit explicit forget_record operations")

    p_serve = sub.add_parser("serve", help="Run the localhost HTTP + MCP server")
    p_serve.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    p_serve.add_argument("--port", type=int, default=8765, help="Bind port (default: 8765)")

    p_mcp = sub.add_parser("mcp-stdio", help="Run MCP JSON-RPC 2.0 over stdio")
    p_mcp.add_argument("--token-env", default="AI_MEMORY_TOKEN", help="Environment variable holding bearer token")

    sub.add_parser("rebuild-index", help="Rebuild SQLite FTS5 and relationship index from vault Markdown")
    sub.add_parser("reconcile", help="Reconcile manual Obsidian edits and new files into SQLite")
    sub.add_parser("run-worker", help="Process all pending consolidation and document-extraction jobs once")
    sub.add_parser("recover", help="Run startup journal recovery and print report")
    sub.add_parser("health", help="Print service health metadata")
    sub.add_parser("list-areas", help="List configured top-level vault areas and subfolders")

    p_area = sub.add_parser("add-area", help="Dynamically add a new top-level area or subfolders")
    p_area.add_argument("slug", help="Lowercase area slug (e.g. academics, finance, health)")
    p_area.add_argument("--folder", default=None, help="Optional Obsidian folder name (defaults to Title-Case)")
    p_area.add_argument("--description", default="", help="Area description for INDEX.md")
    p_area.add_argument("--subfolders", default=None, help="Comma-separated subfolders (e.g. Notes,Projects,Tasks)")
    p_area.add_argument("--no-tasks-dashboard", action="store_true", help="Do not generate an area TASKS.md dashboard")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = MemoryConfig.from_env(data_root=args.data_root)

    if args.command == "init-vault":
        report = init_memory_home(config, overwrite_policy=args.overwrite_policy)
        core = MemoryCore(config, auto_init=False)
        core.index.rebuild_from_vault(config.vault_dir)
        core.refresh_derived_views()
        core.close()
        print(json.dumps(report, indent=2))
        return 0

    if args.command == "create-token":
        core = MemoryCore(config)
        scopes = [s.strip() for s in args.scopes.split(",") if s.strip()]
        identity, plaintext = core.access.create_token(
            label=args.label,
            allowed_scopes=scopes,
            can_write=not args.read_only,
            can_forget=args.allow_forget,
        )
        core.close()
        print(
            json.dumps(
                {
                    "client_id": identity.client_id,
                    "label": identity.label,
                    "allowed_scopes": list(identity.allowed_scopes),
                    "can_write": identity.can_write,
                    "can_forget": identity.can_forget,
                    "bearer_token": plaintext,
                    "note": "Store this token outside the vault; it cannot be displayed again.",
                },
                indent=2,
            )
        )
        return 0

    if args.command == "rebuild-index":
        core = MemoryCore(config)
        count = core.index.rebuild_from_vault(config.vault_dir)
        core.refresh_derived_views()
        core.close()
        print(json.dumps({"reindexed_records": count}))
        return 0

    if args.command == "reconcile":
        core = MemoryCore(config)
        summary = core.index.reconcile_vault(config.vault_dir)
        core.refresh_derived_views()
        core.close()
        print(json.dumps(summary))
        return 0

    if args.command == "run-worker":
        core = MemoryCore(config)
        worker = BackgroundWorker(core)
        outcomes = worker.run_once()
        core.close()
        print(json.dumps({"processed": outcomes}, indent=2))
        return 0

    if args.command == "recover":
        core = MemoryCore(config)
        health = core.health()
        core.close()
        print(json.dumps(health, indent=2))
        return 0

    if args.command == "health":
        core = MemoryCore(config)
        print(json.dumps(core.health(), indent=2))
        core.close()
        return 0

    if args.command == "list-areas":
        core = MemoryCore(config)
        print(json.dumps({"areas": core.areas.list_areas()}, indent=2))
        core.close()
        return 0

    if args.command == "add-area":
        core = MemoryCore(config)
        subs = [s.strip() for s in args.subfolders.split(",") if s.strip()] if args.subfolders else None
        entry = core.areas.register_area(
            slug=args.slug,
            folder=args.folder,
            description=args.description,
            subfolders=subs,
            has_tasks_dashboard=not args.no_tasks_dashboard,
            create_in_vault=True,
        )
        core.refresh_derived_views()
        core.close()
        print(json.dumps(entry, indent=2))
        return 0

    if args.command == "serve":
        core = MemoryCore(config)
        worker = BackgroundWorker(core)
        worker.start()
        server = make_http_server(core, host=args.host, port=args.port)
        print(f"ai-memory listening on http://{args.host}:{args.port} (data_root={config.data_root})", file=sys.stderr)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            worker.stop()
            server.server_close()
            core.close()
        return 0

    if args.command == "mcp-stdio":
        token = os.environ.get(args.token_env)
        core = MemoryCore(config)
        try:
            client = core.access.authenticate(token)
            serve_stdio(core, client)
        finally:
            core.close()
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
