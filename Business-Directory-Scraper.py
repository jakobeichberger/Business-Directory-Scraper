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
    python directory_scraper.py

Metadata:
    File: Business-Directory-Scraper.py
    Author: Jakob
    Maintainer: Jakob
    Email: jakob@eichberger.tech
    Copyright: (c) 2025 Jakob
    License: MIT
    Version: 0.1.0
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
import signal
import logging
import json
import html as html_lib
import requests
import threading
from dataclasses import dataclass
from typing import Optional, List, Set
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
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
BASE_URL = "https://example-directory.tld"
START_URL = "https://example-directory.tld/listings/region"
OUTPUT_XLSX = "business_listings.xlsx"

HTTP_THREADS = 1
REQUEST_TIMEOUT = 25
MAX_RETRIES = 6

# Polite scraping
SLEEP_DETAIL = (0.15, 0.45)

MAX_LISTING_PAGES = 500

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# Example detail page pattern:
#   /company-name_ABC123
DETAIL_PAGE_PATTERN = re.compile(r"^/[a-z0-9-]+_[A-Za-z0-9]+$")


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
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=cfg.request_timeout)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30)
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

    print(f"[ERROR] Failed permanently: {url} -> {last}")
    return None


# -----------------------------
# Listing pages (Playwright)
# -----------------------------
def get_listing_links_playwright(start_url: str) -> List[str]:
    """
    Collect detail page URLs from paginated listing pages using Playwright.

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
        page.set_default_timeout(30000)

        for i in range(1, MAX_LISTING_PAGES + 1):
            url = start_url if i == 1 else f"{start_url}/{i}"
            print(f"[LIST] Loading {url}")

            page.goto(url, wait_until="domcontentloaded")
            page.wait_for_timeout(800)

                soup = make_soup(html)
                page_links: List[str] = []

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if DETAIL_PAGE_PATTERN.match(href):
                    page_links.append(urljoin(BASE_URL, href))

                page_links = list(dict.fromkeys(page_links))

            if not page_links:
                break

            for u in page_links:
                if u not in seen:
                    seen.add(u)
                    links.append(u)

            print(f"[LIST] Page {i}: total links {len(links)}")

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
        ListingEntry: A populated entry containing:
            - url: The input URL.
            - name: Text from the first <h1> element (cleaned), or empty string.
            - address: Heuristically detected street + postal/city line from page text,
              formatted as "line_i, line_{i+1}" (cleaned), or empty string.
            - phone: Value from the first <a href="tel:..."> (cleaned), or empty string.
            - email: Value from the first <a href="mailto:..."> (cleaned), or empty string.
            - website: First external absolute link (href starts with "http" and does not
              contain BASE_URL), or empty string.
    Notes:
        - Uses BeautifulSoup via make_soup() to parse the HTML.
        - Address detection scans consecutive text lines; it looks for a digit in the first
          line and a following line matching r"^\\d{4,5}\\s+\\S+" (e.g., postal code + city).
        - Only the first matching external website link is returned.
    Raises:
        Any exceptions raised by make_soup(), BeautifulSoup accessors, or regular expression
        operations will propagate.
    """
    soup = make_soup(html)
    sel = cfg.field_selectors

    # ── name ──────────────────────────────────────────────────────────────────
    name_el = soup.select_one(sel.get("name") or "h1")
    name = _clean(name_el.get_text(" ", strip=True)) if name_el else ""

    # ── email ─────────────────────────────────────────────────────────────────
    email = ""
    phone = ""
    website = ""

    a_mail = soup.select_one("a[href^='mailto:']")
    if a_mail:
        raw = (a_mail.get("href") or "").replace("mailto:", "")
        email = _normalize_email(raw)

    # ── phone ─────────────────────────────────────────────────────────────────
    phone = ""
    phone_sel = sel.get("phone") or "a[href^='tel:']"
    a_tel = soup.select_one(phone_sel)
    if a_tel:
        phone = _clean(a_tel["href"].replace("tel:", ""))

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("http") and BASE_URL not in href:
            website = href
            break

    text = soup.get_text("\n", strip=True)
    lines = [l for l in text.splitlines() if l.strip()]

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


def fetch_and_parse_listing(url: str) -> Optional[ListingEntry]:
    html = fetch_html(url)
    _sleep(SLEEP_DETAIL)
    return parse_listing_html(url, html) if html else None


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


def autosize_columns(ws, cap: int = 60) -> None:
    """Best-effort column width sizing based on observed content length."""
    for col_idx, _ in enumerate(XLSX_HEADERS, start=1):
        letter = get_column_letter(col_idx)
        max_len = max(
            (len(str(ws.cell(row=r, column=col_idx).value or ""))
             for r in range(1, ws.max_row + 1)),
            default=10,
        )
        ws.column_dimensions[letter].width = min(max_len + 2, cap)


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
    print(f"[INFO] Resume enabled ({len(done_urls)} entries already saved)")

    links = get_listing_links_playwright(cfg)
    todo = [u for u in links if u not in done_urls]

    with ThreadPoolExecutor(max_workers=HTTP_THREADS) as pool:
        futures = [pool.submit(fetch_and_parse_listing, url) for url in todo]

        for i, f in enumerate(as_completed(futures), 1):
            row = f.result()
            if not row:
                continue

            with lock:
                append_row(ws, row)

            if i % 25 == 0:
                wb.save(OUTPUT_XLSX)

    wb.save(OUTPUT_XLSX)
    print(f"[DONE] Finished. Output: {OUTPUT_XLSX}")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal.default_int_handler)
    try:
        main()
    except Exception as e:
        log.exception("Fatal error: %s", e)
        sys.exit(1)
