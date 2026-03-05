# -*- coding: utf-8 -*-
"""
Business Directory Scraper -> Excel / CSV

Generic scraper for business directories with paginated listings
and individual detail pages.

Features:
- External YAML configuration file (config.yaml)
- Structured logging with live shell output and optional log file
- Playwright for JS-rendered listing pages
- Requests + BeautifulSoup for detail pages
- Retry & rate-limit handling
- Resume support (already scraped URLs are skipped)
- Excel export (openpyxl) and/or CSV export
- Configurable CSS selectors — no Python editing required
- Data cleaning and normalisation (phone, email, website, address)
- Missing-field tracking (scraped_at timestamp + missing_fields column)
- Optional DuckDuckGo web search to enrich entries with missing contact data

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
import logging
import re
import sys
import time
import random
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Set
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from concurrent.futures import ThreadPoolExecutor, as_completed

from playwright.sync_api import sync_playwright

try:
    import yaml
    _HAS_YAML = True
except ImportError:
    _HAS_YAML = False

try:
    from duckduckgo_search import DDGS
    _HAS_DDGS = True
except ImportError:
    _HAS_DDGS = False

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

    # Output format: "xlsx", "csv", or "both"
    output_format: str = "xlsx"

    http_threads: int = 1
    request_timeout: int = 25
    max_retries: int = 6
    max_listing_pages: int = 500

    sleep_detail_min: float = 0.15
    sleep_detail_max: float = 0.45

    detail_page_pattern: str = r"^/[a-z0-9-]+_[A-Za-z0-9]+$"

    # CSS selectors for data extraction — set to "" to use the built-in heuristic
    selectors: dict = field(default_factory=lambda: {
        "name": "h1",
        "email": "a[href^='mailto:']",
        "phone": "a[href^='tel:']",
        "website": "",   # empty → first external http link
        "address": "",   # empty → heuristic postal-code scan
    })

    # Set to true to search DuckDuckGo for missing contact data (requires duckduckgo-search)
    enrich_missing_data: bool = False

    headers: dict = field(default_factory=lambda: {
        "User-Agent": "Mozilla/5.0 (compatible; BusinessDirectoryScraper/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    })

    log_level: str = "INFO"
    log_file: str = ""


def load_config(path: str) -> Config:
    """Load a ``Config`` from a YAML file.

    Unknown keys in the file are silently ignored so that adding new
    options to ``config.yaml`` does not break older script versions.
    The ``selectors`` key is merged with the defaults so that users only
    need to override the selectors they care about.

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
        # Merge nested dicts (e.g. selectors, headers) rather than replacing them
        if isinstance(value, dict) and isinstance(getattr(cfg, key), dict):
            merged = dict(getattr(cfg, key))
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
    scraped_at: str = ""
    missing_fields: str = ""

    def compute_missing(self) -> None:
        """Populate ``missing_fields`` with a comma-separated list of core
        fields that are empty, and set ``scraped_at`` to the current UTC time
        if it has not been set yet.
        """
        core_fields = ("name", "address", "phone", "email", "website")
        missing = [f for f in core_fields if not getattr(self, f)]
        self.missing_fields = ", ".join(missing) if missing else ""
        if not self.scraped_at:
            self.scraped_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


# --- Data-cleaning helpers ---

_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def clean_phone(phone: str) -> str:
    """Normalise a phone number string.

    Strips leading/trailing whitespace and collapses internal whitespace.
    Removes any ``tel:`` prefix left over from an ``<a href>`` attribute.
    """
    phone = _clean(re.sub(r"^tel:", "", phone.strip(), flags=re.IGNORECASE))
    # Collapse multiple spaces within the number
    phone = re.sub(r"[\s]+", " ", phone)
    return phone


def clean_email(email: str) -> str:
    """Normalise and validate an email address string.

    Strips whitespace and lower-cases the address.  Returns an empty string
    if the result does not match a basic ``user@domain.tld`` pattern so that
    obviously invalid values are not written to the output.
    """
    email = _clean(re.sub(r"^mailto:", "", email.strip(), flags=re.IGNORECASE)).lower()
    return email if _EMAIL_RE.match(email) else ""


def clean_website(website: str) -> str:
    """Normalise a website URL.

    Strips whitespace.  If the URL has no scheme it is assumed to be
    ``https``.  Returns an empty string for non-HTTP(S) values.
    """
    website = _clean(website)
    if not website:
        return ""
    parsed = urlparse(website)
    if not parsed.scheme:
        website = "https://" + website
        parsed = urlparse(website)
    if parsed.scheme not in ("http", "https"):
        return ""
    return website


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
            page.wait_for_timeout(800)

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

    Extraction uses CSS selectors from ``cfg.selectors`` where provided,
    falling back to built-in heuristics when a selector is empty.

    Selector keys and their default heuristics:

    - **name**: text of the element matched by ``selectors.name`` (default: ``h1``).
    - **email**: ``href`` attribute of ``selectors.email``
      (default: first ``<a href="mailto:…">``).
    - **phone**: ``href`` attribute of ``selectors.phone``
      (default: first ``<a href="tel:…">``).
    - **website**: ``href`` of ``selectors.website``
      (default: first external ``http`` link not on ``cfg.base_url``).
    - **address**: text of ``selectors.address``
      (default: heuristic postal-code scan — a line containing a digit
      followed by a line matching ``^\\d{4,5}\\s+\\S+``).

    All extracted values are cleaned and normalised before being stored.

    Args:
        url:  Canonical URL of the page (stored on the returned entry).
        html: Raw HTML of the page.
        cfg:  Active :class:`Config` (used for ``base_url`` and ``selectors``).

    Returns:
        Populated :class:`ListingEntry` with ``scraped_at`` and
        ``missing_fields`` already computed.
    """
    soup = make_soup(html)
    sel = cfg.selectors

    # --- name ---
    name_selector = sel.get("name") or "h1"
    name_el = soup.select_one(name_selector)
    name = _clean(name_el.get_text(" ", strip=True)) if name_el else ""

    # --- email ---
    email_selector = sel.get("email") or "a[href^='mailto:']"
    a_mail = soup.select_one(email_selector)
    email = clean_email(a_mail["href"]) if a_mail and a_mail.get("href") else ""

    # --- phone ---
    phone_selector = sel.get("phone") or "a[href^='tel:']"
    a_tel = soup.select_one(phone_selector)
    phone = clean_phone(a_tel["href"]) if a_tel and a_tel.get("href") else ""

    # --- website ---
    website = ""
    website_selector = sel.get("website", "")
    if website_selector:
        ws_el = soup.select_one(website_selector)
        if ws_el and ws_el.get("href"):
            website = clean_website(ws_el["href"])
    else:
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            if href.startswith("http") and cfg.base_url not in href:
                website = clean_website(href)
                if website:
                    break

    # --- address ---
    address = ""
    address_selector = sel.get("address", "")
    if address_selector:
        addr_el = soup.select_one(address_selector)
        if addr_el:
            address = _clean(addr_el.get_text(" ", strip=True))
    else:
        lines = [ln for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
        for i in range(len(lines) - 1):
            if re.search(r"\d", lines[i]) and re.match(r"^\d{4,5}\s+\S+", lines[i + 1]):
                address = _clean(lines[i] + ", " + lines[i + 1])
                break

    entry = ListingEntry(
        url=url,
        name=name,
        address=address,
        phone=phone,
        email=email,
        website=website,
    )
    entry.compute_missing()
    return entry


def fetch_and_parse_listing(url: str, cfg: Config) -> Optional[ListingEntry]:
    """Fetch *url* and return a parsed :class:`ListingEntry`, or ``None`` on error.

    If ``cfg.enrich_missing_data`` is ``True`` and the ``duckduckgo-search``
    package is available, a DuckDuckGo search is performed for any missing
    contact fields (email, phone, website) using the business name as the
    query.
    """
    html = fetch_html(url, cfg)
    _sleep(cfg.sleep_detail_min, cfg.sleep_detail_max)
    if not html:
        return None
    entry = parse_listing_html(url, html, cfg)
    if cfg.enrich_missing_data:
        enrich_entry(entry, cfg)
    logger.debug("Parsed: %s — %s", entry.name, url)
    return entry


# -----------------------------
# DuckDuckGo enrichment
# -----------------------------
def enrich_entry(entry: ListingEntry, cfg: Config) -> None:
    """Search DuckDuckGo for any missing contact fields on *entry*.

    Only runs when the ``duckduckgo-search`` package is installed.  Searches
    for ``"<name>" contact email`` (or phone/website depending on which fields
    are missing) and attempts to extract the relevant data from the first few
    search results.

    The entry's ``missing_fields`` list is recomputed after enrichment.

    Args:
        entry: The :class:`ListingEntry` to enrich in place.
        cfg:   Active :class:`Config` (used to log a warning if the package
               is unavailable).
    """
    if not _HAS_DDGS:
        logger.warning(
            "enrich_missing_data is enabled but duckduckgo-search is not installed. "
            "Run: pip install duckduckgo-search"
        )
        return

    if not entry.name:
        return

    missing = [f for f in ("email", "phone", "website") if not getattr(entry, f)]
    if not missing:
        return

    query = f'"{entry.name}" {entry.address or ""} contact'.strip()
    logger.debug("Enriching [%s] via DuckDuckGo: %s", entry.name, query)

    try:
        ddgs = DDGS()
        results = list(ddgs.text(query, max_results=5))
    except Exception as exc:
        logger.debug("DuckDuckGo search failed for %s: %s", entry.name, exc)
        return

    combined_text = " ".join(
        f"{r.get('title', '')} {r.get('body', '')}" for r in results
    )

    if "email" in missing:
        found = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", combined_text)
        for candidate in found:
            cleaned = clean_email(candidate)
            if cleaned:
                entry.email = cleaned
                logger.debug("Enriched email for %s: %s", entry.name, cleaned)
                break

    if "website" in missing:
        for r in results:
            href = r.get("href", "")
            cleaned = clean_website(href)
            if cleaned and cfg.base_url not in cleaned:
                entry.website = cleaned
                logger.debug("Enriched website for %s: %s", entry.name, cleaned)
                break

    if "phone" in missing:
        # Look for common phone number patterns in the combined search snippet text
        found = re.findall(
            r"(?:\+?\d[\d\s\-\(\)]{6,}\d)",
            combined_text,
        )
        for candidate in found:
            cleaned = clean_phone(candidate)
            if len(re.sub(r"\D", "", cleaned)) >= 7:
                entry.phone = cleaned
                logger.debug("Enriched phone for %s: %s", entry.name, cleaned)
                break

    entry.compute_missing()


# -----------------------------
# Excel handling (resume-safe)
# -----------------------------
XLSX_HEADERS = [
    "URL", "Name", "Address", "Phone", "Email", "Website",
    "UID", "Registry Number", "Credit Reference",
    "Scraped At", "Missing Fields",
]


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
        return wb, ws


def read_done_urls(ws) -> Set[str]:
    """Return the set of URLs already present in the worksheet (column A)."""
    return {
        ws.cell(row=r, column=1).value
        for r in range(2, ws.max_row + 1)
        if isinstance(ws.cell(row=r, column=1).value, str)
    }


def append_row(ws, row: ListingEntry) -> None:
    """Append one :class:`ListingEntry` as a new row in the worksheet."""
    ws.append([
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
        row.missing_fields,
    ])


# -----------------------------
# CSV export
# -----------------------------
def export_csv(entries: List[ListingEntry], path: str) -> None:
    """Write *entries* to a UTF-8 CSV file at *path*.

    The file is written from scratch every time this function is called (it is
    not resume-safe like the Excel path, but is intended as an optional
    additional export at the end of the run).

    Args:
        entries: All scraped :class:`ListingEntry` objects.
        path:    Filesystem path for the ``.csv`` file.
    """
    with open(path, "w", newline="", encoding="utf-8-sig") as fh:
        # utf-8-sig writes a BOM so Excel opens the file correctly on Windows
        writer = csv.DictWriter(fh, fieldnames=XLSX_HEADERS)
        writer.writeheader()
        for e in entries:
            writer.writerow({
                "URL": e.url,
                "Name": e.name,
                "Address": e.address,
                "Phone": e.phone,
                "Email": e.email,
                "Website": e.website,
                "UID": e.uid,
                "Registry Number": e.registry_number,
                "Credit Reference": e.credit_reference,
                "Scraped At": e.scraped_at,
                "Missing Fields": e.missing_fields,
            })


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
    5. Fetch and parse each detail page concurrently.
    6. Optionally enrich entries with missing data via DuckDuckGo search.
    7. Append results to the workbook, saving every 25 rows and at the end.
    8. Optionally export a CSV file alongside the Excel file.
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
    logger.info("Output format: %s", cfg.output_format)

    if cfg.enrich_missing_data and not _HAS_DDGS:
        logger.warning(
            "enrich_missing_data is enabled but duckduckgo-search is not installed. "
            "Enrichment will be skipped. Run: pip install duckduckgo-search"
        )

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
    all_entries: List[ListingEntry] = []

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
                all_entries.append(row)

            missing_note = f" [missing: {row.missing_fields}]" if row.missing_fields else ""
            logger.info("[%d/%d] Saved: %s%s", completed, total,
                        row.name or futures[future], missing_note)

            if completed % 25 == 0:
                with lock:
                    wb.save(cfg.output_xlsx)
                logger.debug("Progress checkpoint saved (%d rows)", completed)

    wb.save(cfg.output_xlsx)
    logger.info("Done — %d listings written to %s", completed, cfg.output_xlsx)

    if cfg.output_format in ("csv", "both"):
        csv_path = str(Path(cfg.output_xlsx).with_suffix(".csv"))
        export_csv(all_entries, csv_path)
        logger.info("CSV export written to %s", csv_path)


if __name__ == "__main__":
    main()
