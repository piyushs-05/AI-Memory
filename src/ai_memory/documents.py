"""Bounded PDF/text extraction, OCR fallback, and attachment cleanup (Design Section 13, R14/R19)."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from .config import MemoryConfig
from .ids import utc_now_iso
from .storage import atomic_write_json, ensure_within

MAX_EXTRACTION_TIMEOUT_SECS = 30
MAX_OCR_PAGES = 20
MAX_EXCERPT_CHARS_IN_NOTE = 20_000


def extraction_cache_path(config: MemoryConfig, doc_id: str) -> Path:
    return ensure_within(config.runtime_dir, config.runtime_dir / "extractions" / f"{doc_id}.json")


def delete_document_artifacts(
    config: MemoryConfig, *, doc_id: str, original_attachment_path: str | None
) -> dict[str, bool]:
    """Delete original binary attachment and any cached extraction output (R14)."""
    removed_original = False
    removed_cache = False

    if original_attachment_path:
        att_path = ensure_within(config.records_dir, config.records_dir / original_attachment_path)
        if att_path.is_file():
            att_path.unlink()
            removed_original = True

    cache_path = extraction_cache_path(config, doc_id)
    if cache_path.is_file():
        cache_path.unlink()
        removed_cache = True

    return {"removed_original": removed_original, "removed_cache": removed_cache}


def extract_document_content(
    config: MemoryConfig,
    *,
    doc_id: str,
    original_attachment_path: str,
    media_type: str,
) -> dict[str, Any]:
    """
    Extract page-referenced text from an ingested document with bounded timeouts.
    Preserves the original binary untouched.
    """
    att_path = ensure_within(config.records_dir, config.records_dir / original_attachment_path)
    if not att_path.is_file():
        return {
            "extraction_status": "failed",
            "page_count": None,
            "pages": [],
            "error": "Original attachment file missing",
        }

    if media_type.startswith("text/") or att_path.suffix.lower() in (".txt", ".md", ".csv", ".json"):
        try:
            text = att_path.read_text(encoding="utf-8", errors="replace")[:MAX_EXCERPT_CHARS_IN_NOTE]
            result = {
                "doc_id": doc_id,
                "extraction_status": "extracted",
                "page_count": 1,
                "pages": [{"page": 1, "text": text}],
                "extracted_at": utc_now_iso(),
                "error": None,
            }
            atomic_write_json(extraction_cache_path(config, doc_id), result)
            return result
        except Exception as exc:
            return {"extraction_status": "failed", "page_count": None, "pages": [], "error": str(exc)}

    if media_type == "application/pdf" or att_path.suffix.lower() == ".pdf":
        return _extract_pdf(config, doc_id=doc_id, pdf_path=att_path)

    return {
        "doc_id": doc_id,
        "extraction_status": "skipped",
        "page_count": None,
        "pages": [],
        "extracted_at": utc_now_iso(),
        "error": f"Unsupported media type for text extraction: {media_type}",
    }


def _extract_pdf(config: MemoryConfig, *, doc_id: str, pdf_path: Path) -> dict[str, Any]:
    pdftotext = shutil.which("pdftotext")
    pdfinfo = shutil.which("pdfinfo")
    page_count: int | None = None

    if pdfinfo:
        try:
            info = subprocess.run(
                [pdfinfo, str(pdf_path)],
                capture_output=True,
                text=True,
                timeout=MAX_EXTRACTION_TIMEOUT_SECS,
                check=False,
            )
            if info.returncode == 0:
                m = re.search(r"^Pages:\s+(\d+)", info.stdout, flags=re.MULTILINE)
                if m:
                    page_count = int(m.group(1))
        except Exception:
            pass

    pages: list[dict[str, Any]] = []
    if pdftotext:
        try:
            proc = subprocess.run(
                [pdftotext, "-enc", "UTF-8", str(pdf_path), "-"],
                capture_output=True,
                text=True,
                timeout=MAX_EXTRACTION_TIMEOUT_SECS,
                check=False,
            )
            if proc.returncode == 0:
                raw_pages = proc.stdout.split("\f")
                for idx, chunk in enumerate(raw_pages, start=1):
                    cleaned = chunk.strip()
                    if cleaned:
                        pages.append({"page": idx, "text": cleaned})
                if page_count is None and pages:
                    page_count = len(pages)
            else:
                return {
                    "doc_id": doc_id,
                    "extraction_status": "failed",
                    "page_count": page_count,
                    "pages": [],
                    "extracted_at": utc_now_iso(),
                    "error": f"pdftotext failed: {proc.stderr.strip()[:300]}",
                }
        except subprocess.TimeoutExpired:
            return {
                "doc_id": doc_id,
                "extraction_status": "failed",
                "page_count": page_count,
                "pages": [],
                "extracted_at": utc_now_iso(),
                "error": "pdftotext timed out",
            }

    if pages:
        result = {
            "doc_id": doc_id,
            "extraction_status": "extracted",
            "page_count": page_count or len(pages),
            "pages": pages,
            "extracted_at": utc_now_iso(),
            "error": None,
        }
        atomic_write_json(extraction_cache_path(config, doc_id), result)
        return result

    # OCR fallback when pdftotext returned no text (scanned PDF)
    pdftoppm = shutil.which("pdftoppm")
    tesseract = shutil.which("tesseract")
    if pdftoppm and tesseract:
        ocr_pages = _ocr_pdf(pdf_path, pdftoppm=pdftoppm, tesseract=tesseract, max_pages=MAX_OCR_PAGES)
        if ocr_pages:
            result = {
                "doc_id": doc_id,
                "extraction_status": "ocr_extracted",
                "page_count": page_count or len(ocr_pages),
                "pages": ocr_pages,
                "extracted_at": utc_now_iso(),
                "error": None,
            }
            atomic_write_json(extraction_cache_path(config, doc_id), result)
            return result

    status = "failed" if not pdftotext else "extracted"
    err = "No PDF text extractor (pdftotext) available on host" if not pdftotext else None
    result = {
        "doc_id": doc_id,
        "extraction_status": status,
        "page_count": page_count,
        "pages": [],
        "extracted_at": utc_now_iso(),
        "error": err,
    }
    atomic_write_json(extraction_cache_path(config, doc_id), result)
    return result


def _ocr_pdf(pdf_path: Path, *, pdftoppm: str, tesseract: str, max_pages: int) -> list[dict[str, Any]]:
    ocr_pages: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="aim-ocr-") as tmpdir:
        prefix = str(Path(tmpdir) / "page")
        try:
            subprocess.run(
                [pdftoppm, "-png", "-r", "150", "-l", str(max_pages), str(pdf_path), prefix],
                capture_output=True,
                timeout=MAX_EXTRACTION_TIMEOUT_SECS,
                check=True,
            )
        except Exception:
            return []
        for idx, img_path in enumerate(sorted(Path(tmpdir).glob("page-*.png")), start=1):
            try:
                proc = subprocess.run(
                    [tesseract, str(img_path), "stdout", "-l", "eng"],
                    capture_output=True,
                    text=True,
                    timeout=MAX_EXTRACTION_TIMEOUT_SECS,
                    check=False,
                )
                if proc.returncode == 0 and proc.stdout.strip():
                    ocr_pages.append({"page": idx, "text": proc.stdout.strip()})
            except Exception:
                continue
    return ocr_pages
