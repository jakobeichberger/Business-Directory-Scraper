# -*- coding: utf-8 -*-
"""
Business Directory Scraper -> Excel / CSV

Generic scraper for business directories with paginated listings
and individual detail pages.

Features:
- External YAML configuration file (config.yaml)
- Configurable CSS field selectors (no Python edits needed for new sites)
- Structured logging with live shell output and optional log file
- Playwright for JS-rendered listing pages
- Requests + BeautifulSoup for detail pages
- Retry & rate-limit handling
- Resume support (already scraped URLs are skipped)
- Excel export (openpyxl) and optional CSV export
- Data normalisation (phone, e-mail, URL)
- Data-quality score per row (% of key fields filled)
- Optional DuckDuckGo enrichment for missing contact fields

Requirements:
    pip install -r requirements.txt
    playwright install chromium

Usage:
    python Business-Directory-Scraper.py
    python Business-Directory-Scraper.py --config my_config.yaml
    python Business-Directory-Scraper.py --config config.yaml --log-level DEBUG

Metadata:
    File: Business-Directory-Scraper.py
    Author: Jakob
    Maintainer: Jakob
    Email: jakob@eichberger.tech
    Copyright: (c) 2025 Jakob
    License: MIT
    Version: 0.3.0
    Status: Development
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, List, Set
from urllib.parse import urljoin, quote as urlquote

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from openpyxl.styles import PatternFill, Font
from concurrent.futures import ThreadPoolExecutor, as_completed

from playwright.sync_api import sync_playwright

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

# Module-level logger — configured in setup_logging()
logger = logging.getLogger("scraper")


# -----------------------------
# Configuration dataclass
# -----------------------------
@dataclass
class Config:
    """All tunable settings for the scraper.  Values are loaded from a YAML
    file at runtime; see ``config.yaml`` for the canonical reference."""

    base_url: str = "https://example-directory.tld"
    start_url: str = "https://example-directory.tld/listings/region"
    output_xlsx: str = "business_listings.xlsx"
    output_csv: str = ""  # Leave empty to skip CSV output

    http_threads: int = 1
    request_timeout: int = 25
    max_retries: int = 6
    max_listing_pages: int = 500

    sleep_detail_min: float = 0.15
    sleep_detail_max: float = 0.45

    # Milliseconds Playwright waits after page load before reading the DOM
    playwright_wait_ms: int = 800

    detail_page_pattern: str = r"^/[a-z0-9-]+_[A-Za-z0-9]+$"

    # CSS selectors used by parse_listing_html() — customise per site without
    # touching the Python source.
    field_selectors: dict = field(default_factory=lambda: {
        "name":    "h1",
        "email":   "a[href^='mailto:']",
        "phone":   "a[href^='tel:']",
        "website": "a[href^='http']",
        "address": "",   # leave empty to use the built-in heuristic
    })

    headers: dict = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (compatible; BusinessDirectoryScraper/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })

    log_level: str = "INFO"
    log_file: str = ""

    # When True, use DuckDuckGo to attempt to fill in missing contact fields
    enrich_missing_data: bool = False


def load_config(path: str) -> Config:
    """Load a ``Config`` from a YAML file.

    Unknown keys in the file are silently ignored so that adding new
    options to ``config.yaml`` does not break older script versions.
    Nested dict keys (e.g. ``field_selectors``, ``headers``) are merged
    rather than replaced so that users only need to specify the keys they
    want to override.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        A populated :class:`Config` instance.

    Raises:
        SystemExit: If the file cannot be read or parsed.
    """
    if not _HAS_YAML:
        logger.warning("PyYAML is not installed — using built-in defaults. "
                       "Run: pip install pyyaml")
        return Config()

    try:
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
    except FileNotFoundError:
        logger.error("Config file not found: %s", path)
        sys.exit(1)
    except yaml.YAMLError as exc:
        logger.error("Failed to parse config file %s: %s", path, exc)
        sys.exit(1)

    cfg = Config()
    for key, value in data.items():
        if not hasattr(cfg, key):
            continue
        existing = getattr(cfg, key)
        if isinstance(existing, dict) and isinstance(value, dict):
            # Merge nested dicts (only override the keys that are present in YAML)
            merged = dict(existing)
            merged.update(value)
            setattr(cfg, key, merged)
        else:
            setattr(cfg, key, value)
    return cfg


# -----------------------------
# Logging setup
# -----------------------------
def setup_logging(level: str, log_file: str = "") -> None:
    """Configure the root *scraper* logger with a console handler and an
    optional file handler.

    Args:
        level:    Log level string (``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``).
        log_file: If non-empty, log messages are also written to this file.
    """
    numeric = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric)

    fmt = logging.Formatter(
        "[%(levelname)-8s] %(asctime)s  %(message)s",
        datefmt="%H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info("Logging to file: %s", log_file)


# -----------------------------
# Data model
# -----------------------------
# Fields that count towards the data-quality score (any non-empty value = +1)
_QUALITY_FIELDS = ("name", "address", "phone", "email", "website")


@dataclass
class ListingEntry:
    """One scraped business listing."""

    url: str
    name: str = ""
    address: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    uid: str = ""
    registry_number: str = ""
    credit_reference: str = ""
    scraped_at: str = ""   # ISO-8601 UTC timestamp set at parse time
    enriched: bool = False  # True when at least one field was filled by web search

    @property
    def data_quality(self) -> int:
        """Return the percentage (0-100) of key contact fields that are filled."""
        filled = sum(1 for f in _QUALITY_FIELDS if getattr(self, f))
        return round(filled / len(_QUALITY_FIELDS) * 100)


# -----------------------------
# Utilities
# -----------------------------
def _sleep(min_s: float, max_s: float) -> None:
    """Sleep for a random duration in [min_s, max_s] seconds."""
    time.sleep(random.uniform(min_s, max_s))


def make_soup(html: str) -> BeautifulSoup:
    """Return a BeautifulSoup object, preferring lxml over html.parser."""
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def _clean(s: str) -> str:
    """Collapse whitespace and strip a string."""
    return re.sub(r"\s+", " ", (s or "")).strip()


def _normalize_phone(raw: str) -> str:
    """Return a lightly cleaned phone string.

    Keeps digits, ``+``, spaces, hyphens, parentheses, dots, and the letters
    that commonly appear in extension markers (e, x, t for "ext").  Strips
    everything else.  Empty input returns an empty string.
    """
    if not raw:
        return ""
    # Keep: digits, +, whitespace, hyphen, parentheses, dot, and letters e/x/t
    # (individual characters in the class, not the word "ext")
    cleaned = re.sub(r"[^\d+\s\-().ext]", "", raw, flags=re.I).strip()
    return cleaned if re.search(r"\d{3,}", cleaned) else ""


def _normalize_email(raw: str) -> str:
    """Return a lowercase, stripped e-mail address or an empty string."""
    candidate = _clean(raw).lower()
    # Simple structural validation — must contain exactly one @
    return candidate if re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", candidate) else ""


def _normalize_url(raw: str) -> str:
    """Ensure a URL starts with a scheme; return an empty string if it looks invalid."""
    url = _clean(raw)
    if not url:
        return ""
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
    # Very loose check — must have at least one non-special char, a literal dot,
    # and more non-whitespace chars after (e.g. http://example.com)
    return url if re.search(r"https?://[^\s/$.?#]+\.[^\s]+", url, re.I) else ""


# -----------------------------
# HTTP (detail pages)
# -----------------------------
_thread_local = threading.local()


def _get_session(headers: dict) -> requests.Session:
    """Return (or create) a :class:`requests.Session` for the current thread.

    Using thread-local storage ensures that each worker thread in the pool
    has its own session, avoiding race conditions on shared session state
    (cookies, adapters, etc.) while still reusing connections within a thread.
    """
    if not hasattr(_thread_local, "session"):
        session = requests.Session()
        session.headers.update(headers)
        _thread_local.session = session
    return _thread_local.session


def fetch_html(url: str, cfg: Config) -> Optional[str]:
    """Fetch and return HTML for *url* with retry / back-off logic.

    Retries up to ``cfg.max_retries`` times.  On HTTP 429 the ``Retry-After``
    header is respected; for other errors exponential back-off with jitter is
    used.  Uses a thread-local :class:`requests.Session` so that concurrent
    workers do not share session state.

    Args:
        url: Target URL.
        cfg: Active :class:`Config`.

    Returns:
        Response body as text, or ``None`` if all retries are exhausted.
    """
    session = _get_session(cfg.headers)
    last_exc = None
    for attempt in range(1, cfg.max_retries + 1):
        try:
            r = session.get(url, timeout=cfg.request_timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After", "")
                wait = float(retry_after) if retry_after.isdigit() else min(2 ** attempt, 30)
                logger.warning("Rate-limited (429) on %s — waiting %.1fs", url, wait)
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.text

        except requests.RequestException as exc:
            last_exc = exc
            backoff = min(2 ** attempt, 20) + random.uniform(0, 1.0)
            logger.debug("Attempt %d/%d failed for %s: %s — retrying in %.1fs",
                         attempt, cfg.max_retries, url, exc, backoff)
            time.sleep(backoff)

    logger.error("Permanently failed: %s — %s", url, last_exc)
    return None


# -----------------------------
# Listing pages (Playwright)
# -----------------------------
def get_listing_links_playwright(cfg: Config) -> List[str]:
    """Collect detail-page URLs from paginated listing pages using Playwright.

    Navigates pages 1 … ``cfg.max_listing_pages`` and extracts ``<a href>``
    links that match ``cfg.detail_page_pattern``.  Stops early when a page
    yields no new links.

    Args:
        cfg: Active :class:`Config`.

    Returns:
        Ordered list of unique absolute detail-page URLs.
    """
    pattern = re.compile(cfg.detail_page_pattern)
    links: List[str] = []
    seen: Set[str] = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.set_default_timeout(30_000)

        for i in range(1, cfg.max_listing_pages + 1):
            url = cfg.start_url if i == 1 else f"{cfg.start_url}/{i}"
            logger.info("[LIST] Page %d — loading %s", i, url)

            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(cfg.playwright_wait_ms)

            soup = make_soup(page.content())
            page_links: List[str] = []

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if pattern.match(href):
                    page_links.append(urljoin(cfg.base_url, href))

            # de-dupe while preserving order
            page_links = list(dict.fromkeys(page_links))

            if not page_links:
                logger.info("[LIST] No links found on page %d — stopping", i)
                break

            new = [u for u in page_links if u not in seen]
            seen.update(new)
            links.extend(new)

            logger.info("[LIST] Page %d: +%d new links (total: %d)", i, len(new), len(links))

        browser.close()

    return links


# -----------------------------
# Detail page parser
# -----------------------------
def parse_listing_html(url: str, html: str, cfg: Config) -> ListingEntry:
    """Parse a business listing HTML page and extract key contact fields.

    Extraction strategy (all selectors are configurable via ``cfg.field_selectors``):

    - **name**: text of the element matching the ``name`` selector (default ``h1``).
    - **email**: ``href`` of the first element matching the ``email`` selector.
    - **phone**: ``href`` of the first element matching the ``phone`` selector.
    - **website**: first external ``http`` link that does not belong to the
      directory's own domain, matched via the ``website`` selector.
    - **address**: if ``address`` selector is set, text of the matching element;
      otherwise a heuristic scan — looks for a line containing a digit followed
      by a line matching ``^\\d{4,5}\\s+\\S+`` (postal code + city).

    All fields are normalised (phone via :func:`_normalize_phone`, email via
    :func:`_normalize_email`, website via :func:`_normalize_url`) and stripped
    of redundant whitespace.

    Args:
        url:  Canonical URL of the page (stored on the returned entry).
        html: Raw HTML of the page.
        cfg:  Active :class:`Config` (used for ``base_url`` and selectors).

    Returns:
        Populated :class:`ListingEntry` with a UTC ``scraped_at`` timestamp.
    """
    soup = make_soup(html)
    sel = cfg.field_selectors

    # ── name ──────────────────────────────────────────────────────────────────
    name_el = soup.select_one(sel.get("name") or "h1")
    name = _clean(name_el.get_text(" ", strip=True)) if name_el else ""

    # ── email ─────────────────────────────────────────────────────────────────
    email = ""
    email_sel = sel.get("email") or "a[href^='mailto:']"
    a_mail = soup.select_one(email_sel)
    if a_mail:
        raw = (a_mail.get("href") or "").replace("mailto:", "")
        email = _normalize_email(raw)

    # ── phone ─────────────────────────────────────────────────────────────────
    phone = ""
    phone_sel = sel.get("phone") or "a[href^='tel:']"
    a_tel = soup.select_one(phone_sel)
    if a_tel:
        raw = (a_tel.get("href") or "").replace("tel:", "")
        phone = _normalize_phone(raw)

    # ── website ───────────────────────────────────────────────────────────────
    website = ""
    website_sel = sel.get("website") or "a[href^='http']"
    for a in soup.select(website_sel):
        href = (a.get("href") or "").strip()
        if href.startswith("http") and cfg.base_url not in href:
            website = _normalize_url(href)
            if website:
                break

    # ── address ───────────────────────────────────────────────────────────────
    address = ""
    address_sel = sel.get("address") or ""
    if address_sel:
        addr_el = soup.select_one(address_sel)
        if addr_el:
            address = _clean(addr_el.get_text(" ", strip=True))
    else:
        # Built-in heuristic: street line followed by postal-code + city line
        lines = [ln for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
        for i in range(len(lines) - 1):
            if re.search(r"\d", lines[i]) and re.match(r"^\d{4,5}\s+\S+", lines[i + 1]):
                address = _clean(lines[i] + ", " + lines[i + 1])
                break

    return ListingEntry(
        url=url,
        name=name,
        address=address,
        phone=phone,
        email=email,
        website=website,
        scraped_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    )


def fetch_and_parse_listing(url: str, cfg: Config) -> Optional[ListingEntry]:
    """Fetch *url* and return a parsed :class:`ListingEntry`, or ``None`` on error."""
    html = fetch_html(url, cfg)
    _sleep(cfg.sleep_detail_min, cfg.sleep_detail_max)
    if not html:
        return None
    entry = parse_listing_html(url, html, cfg)
    if cfg.enrich_missing_data:
        entry = enrich_entry(entry, cfg)
    logger.debug("Parsed: %s — %s (quality: %d%%)", entry.name, url, entry.data_quality)
    return entry


# -----------------------------
# Optional web-search enrichment
# -----------------------------
def _ddg_search(query: str, cfg: Config) -> str:
    """Query the DuckDuckGo Instant Answer API and return the best text snippet.

    Uses the free, no-key-required ``/`` JSON endpoint.  Returns an empty
    string when the API call fails or returns nothing useful.

    Args:
        query: Free-text search query.
        cfg:   Active :class:`Config` (for HTTP settings).

    Returns:
        The ``AbstractText`` or ``Answer`` from the DuckDuckGo response, or
        an empty string.
    """
    url = (
        "https://api.duckduckgo.com/?"
        f"q={urlquote(query, safe='')}&format=json&no_html=1&skip_disambig=1"
    )
    raw = fetch_html(url, cfg)
    if not raw:
        return ""
    try:
        data = json.loads(raw)
        return data.get("AbstractText") or data.get("Answer") or ""
    except (json.JSONDecodeError, AttributeError):
        return ""


def enrich_entry(entry: ListingEntry, cfg: Config) -> ListingEntry:
    """Attempt to fill missing contact fields via a DuckDuckGo web search.

    Only runs when ``cfg.enrich_missing_data`` is ``True`` and the entry has
    a name.  Only missing fields are searched for — already-populated fields
    are never overwritten.

    Args:
        entry: The :class:`ListingEntry` to enrich (modified in place).
        cfg:   Active :class:`Config`.

    Returns:
        The (possibly updated) :class:`ListingEntry`.
    """
    if not entry.name:
        return entry

    missing = [f for f in ("phone", "email", "website") if not getattr(entry, f)]
    if not missing:
        return entry

    query = f"{entry.name} {entry.address or ''} {' '.join(missing)} contact".strip()
    snippet = _ddg_search(query, cfg)
    if not snippet:
        logger.debug("Enrichment: no DDG result for '%s'", entry.name)
        return entry

    changed = False

    if not entry.phone:
        m = re.search(r"(?:phone|tel)[:\s]*(\+?[\d][\d\s\-().]{5,18}\d)", snippet, re.I)
        if m:
            candidate = _normalize_phone(m.group(1))
            if candidate:
                entry.phone = candidate
                changed = True

    if not entry.email:
        # Extract candidate with a broad pattern, then validate via _normalize_email
        m = re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", snippet)
        if m:
            candidate = _normalize_email(m.group(0))
            if candidate:
                entry.email = candidate
                changed = True

    if not entry.website:
        m = re.search(r"https?://[^\s<>\"']+", snippet)
        if m:
            candidate = _normalize_url(m.group(0))
            if candidate and cfg.base_url not in candidate:
                entry.website = candidate
                changed = True

    if changed:
        entry.enriched = True
        logger.debug("Enrichment: filled fields for '%s'", entry.name)

    return entry


# -----------------------------
# Excel handling (resume-safe)
# -----------------------------
XLSX_HEADERS = [
    "URL", "Name", "Address", "Phone", "Email", "Website",
    "UID", "Registry Number", "Credit Reference",
    "Scraped At", "Data Quality (%)", "Enriched",
]

# Light-yellow fill for rows where data quality < 60 %
_LOW_QUALITY_FILL = PatternFill(start_color="FFF9C4", end_color="FFF9C4", fill_type="solid")
_HEADER_FONT = Font(bold=True)


def load_or_create_workbook(path: str):
    """Load an existing workbook from *path* or create a new one with headers.

    Args:
        path: Filesystem path to the ``.xlsx`` file.

    Returns:
        Tuple ``(wb, ws)`` — the workbook and its active worksheet.

    Raises:
        Any :mod:`openpyxl` exception other than :exc:`FileNotFoundError`.
    """
    try:
        wb = load_workbook(path)
        ws = wb.active
        return wb, ws
    except FileNotFoundError:
        wb = Workbook()
        ws = wb.active
        ws.append(XLSX_HEADERS)
        for col, _ in enumerate(XLSX_HEADERS, start=1):
            ws.cell(row=1, column=col).font = _HEADER_FONT
        return wb, ws


def read_done_urls(ws) -> Set[str]:
    """Return the set of URLs already present in the worksheet (column A)."""
    return {
        ws.cell(row=r, column=1).value
        for r in range(2, ws.max_row + 1)
        if isinstance(ws.cell(row=r, column=1).value, str)
    }


def append_row(ws, row: ListingEntry) -> None:
    """Append one :class:`ListingEntry` as a new row in the worksheet.

    Rows with a data-quality score below 60 % are highlighted in light yellow
    so they are easy to spot and verify manually.
    """
    values = [
        row.url,
        row.name,
        row.address,
        row.phone,
        row.email,
        row.website,
        row.uid,
        row.registry_number,
        row.credit_reference,
        row.scraped_at,
        row.data_quality,
        "Yes" if row.enriched else "No",
    ]
    ws.append(values)
    if row.data_quality < 60:
        new_row = ws.max_row
        for col in range(1, len(XLSX_HEADERS) + 1):
            ws.cell(row=new_row, column=col).fill = _LOW_QUALITY_FILL


# -----------------------------
# CSV export
# -----------------------------
def save_csv(entries: List[ListingEntry], path: str) -> None:
    """Write *entries* to a UTF-8 CSV file at *path*.

    The file is written fresh each time (not appended) so it always reflects
    the complete result set from the current run.  Use the Excel file for
    incremental / resume runs.

    Args:
        entries: List of :class:`ListingEntry` objects to write.
        path:    Filesystem path for the output ``.csv`` file.
    """
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(XLSX_HEADERS)
        for e in entries:
            writer.writerow([
                e.url, e.name, e.address, e.phone, e.email, e.website,
                e.uid, e.registry_number, e.credit_reference,
                e.scraped_at, e.data_quality, "Yes" if e.enriched else "No",
            ])
    logger.info("CSV saved: %s (%d rows)", path, len(entries))


# -----------------------------
# Main
# -----------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Business Directory Scraper — scrapes listings to Excel",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="FILE",
        help="Path to the YAML configuration file",
    )
    parser.add_argument(
        "--log-level",
        default=None,
        metavar="LEVEL",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Override the log level from the config file",
    )
    return parser.parse_args()


def main() -> None:
    """Orchestrate the full scraping workflow.

    1. Parse CLI arguments and load the YAML config.
    2. Configure logging (console + optional file).
    3. Load or create the output Excel workbook.
    4. Collect listing URLs via Playwright (skipping already-done ones).
    5. Fetch and parse each detail page concurrently (with optional enrichment).
    6. Append results to the workbook, saving every 25 rows and at the end.
    7. Optionally write a CSV file when ``output_csv`` is set in config.
    """
    args = parse_args()

    # Bootstrap logging early so config-load errors are visible
    setup_logging("INFO")

    cfg = load_config(args.config)

    # CLI --log-level overrides the config file value
    if args.log_level:
        cfg.log_level = args.log_level

    # Re-configure logging with the final level and optional file
    logger.handlers.clear()
    setup_logging(cfg.log_level, cfg.log_file)

    logger.info("Config loaded from: %s", args.config)
    logger.info("Output file: %s", cfg.output_xlsx)
    if cfg.output_csv:
        logger.info("CSV output: %s", cfg.output_csv)
    if cfg.enrich_missing_data:
        logger.info("DuckDuckGo enrichment: enabled")

    wb, ws = load_or_create_workbook(cfg.output_xlsx)
    lock = threading.Lock()

    done_urls = read_done_urls(ws)
    logger.info("Resume: %d entries already saved", len(done_urls))

    links = get_listing_links_playwright(cfg)
    todo = [u for u in links if u not in done_urls]
    logger.info("Listings discovered: %d total, %d pending", len(links), len(todo))

    if not todo:
        logger.info("Nothing to do — all listings already scraped.")
        return

    completed = 0
    total = len(todo)
    csv_entries: List[ListingEntry] = []

    with ThreadPoolExecutor(max_workers=cfg.http_threads) as pool:
        futures = {pool.submit(fetch_and_parse_listing, url, cfg): url
                   for url in todo}

        for future in as_completed(futures):
            completed += 1
            row = future.result()
            if not row:
                logger.warning("[%d/%d] Failed: %s", completed, total, futures[future])
                continue

            with lock:
                append_row(ws, row)
                if cfg.output_csv:
                    csv_entries.append(row)

            enriched_tag = " [enriched]" if row.enriched else ""
            logger.info(
                "[%d/%d] Saved: %s  quality=%d%%%s",
                completed, total,
                row.name or futures[future],
                row.data_quality,
                enriched_tag,
            )

            if completed % 25 == 0:
                with lock:
                    wb.save(cfg.output_xlsx)
                logger.debug("Progress checkpoint saved (%d rows)", completed)

    wb.save(cfg.output_xlsx)
    logger.info("Done — %d listings written to %s", completed, cfg.output_xlsx)

    if cfg.output_csv and csv_entries:
        save_csv(csv_entries, cfg.output_csv)


if __name__ == "__main__":
    main()
