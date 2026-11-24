"""Lightweight localhost HTTP + MCP Streamable HTTP server with Bearer token auth."""

from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .core import MemoryCore
from .errors import MemoryServiceError
from .mcp_server import handle_jsonrpc

MAX_REQUEST_BYTES = 32 * 1024 * 1024  # 32 MiB cap


def _extract_bearer(auth_header: str | None) -> str | None:
    if not auth_header:
        return None
    parts = auth_header.strip().split(" ", 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return None


def make_http_server(core: MemoryCore, *, host: str | None = None, port: int | None = None) -> ThreadingHTTPServer:
    bind_host = host or core.config.http_host
    bind_port = port if port is not None else core.config.http_port

    class MemoryRequestHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            # Default to operational metadata only; never log personal content
            return

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self) -> dict[str, Any]:
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0 or length > MAX_REQUEST_BYTES:
                raise MemoryServiceError("validation_failed", "Invalid or oversized request body")
            raw = self.rfile.read(length)
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except Exception as exc:
                raise MemoryServiceError("validation_failed", f"Malformed JSON body: {exc}") from exc
            if not isinstance(parsed, dict):
                raise MemoryServiceError("validation_failed", "Request body must be a JSON object")
            return parsed

        def do_GET(self) -> None:
            if self.path == "/health":
                self._send_json(HTTPStatus.OK, core.health())
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": {"code": "not_found_or_unavailable", "message": "Not found"}})

        def do_POST(self) -> None:
            try:
                token = _extract_bearer(self.headers.get("Authorization"))
                client = core.access.authenticate(token)
                body = self._read_json()
            except MemoryServiceError as exc:
                self._send_json(
                    exc.http_status,
                    {"ok": False, "error": exc.to_dict(), "policy_version": core.config.policy_version},
                )
                return

            if self.path == "/v1/ops":
                envelope = core.dispatch(
                    client=client,
                    operation=str(body.get("operation") or ""),
                    payload=body.get("input"),
                    context_id=body.get("context_id"),
                    idempotency_key=body.get("idempotency_key"),
                )
                status = HTTPStatus.OK
                if not envelope.get("ok"):
                    code = envelope.get("error", {}).get("code", "validation_failed")
                    status = MemoryServiceError(code, "").http_status
                self._send_json(status, envelope)
                return

            if self.path == "/mcp":
                resp = handle_jsonrpc(core, client, body)
                if resp is None:
                    self.send_response(HTTPStatus.ACCEPTED)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self._send_json(HTTPStatus.OK, resp)
                return

            self._send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": {"code": "not_found_or_unavailable", "message": "Not found"}})

    return ThreadingHTTPServer((bind_host, bind_port), MemoryRequestHandler)
