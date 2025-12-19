# -*- coding: utf-8 -*-
"""
Business Directory Scraper -> Excel

Generic scraper for business directories with paginated listings
and individual detail pages.

Features:
- Playwright for JS-rendered listing pages (consent-safe)
- Requests + BeautifulSoup for detail pages
- Retry & rate-limit handling
- Resume support (already scraped URLs are skipped)
- Excel export (openpyxl)

Requirements:
    pip install requests beautifulsoup4 openpyxl playwright
    playwright install chromium

Usage:
    python directory_scraper.py

Metadata:
    File: Business-Directory-Scraper.py
    Author: Jakob
    Maintainer: Jakob
    Email: jakob@€ichberger.tech
    Copyright: (c) 2025 Jakob
    License: MIT
    Version: 0.1.0
    Status: Development
"""

from __future__ import annotations

import re
import time
import random
import threading
from dataclasses import dataclass
from typing import Optional, List, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from openpyxl import Workbook, load_workbook
from concurrent.futures import ThreadPoolExecutor, as_completed

from playwright.sync_api import sync_playwright


# -----------------------------
# Configuration
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
@dataclass
class ListingEntry:
    url: str
    name: str = ""
    address: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    uid: str = ""
    registry_number: str = ""
    credit_reference: str = ""


# -----------------------------
# Utilities
# -----------------------------
def _sleep(rng):
    time.sleep(random.uniform(rng[0], rng[1]))

def make_soup(html: str) -> BeautifulSoup:
    """Use lxml if available, fallback to html.parser."""
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")

def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


# -----------------------------
# HTTP (detail pages)
# -----------------------------
_session = requests.Session()
_session.headers.update(HEADERS)

def fetch_html(url: str) -> Optional[str]:
    """
    Fetch and return the HTML content for a given URL with retry and backoff logic.
    Performs an HTTP GET request using a shared session and retries on transient
    failures up to `MAX_RETRIES`. If the server responds with HTTP 429 (Too Many
    Requests), the function respects the `Retry-After` header when it is a digit;
    otherwise it waits using exponential backoff capped at 30 seconds. For other
    request exceptions, it retries with exponential backoff (capped at 20 seconds)
    plus a small random jitter.
    Args:
        url: The URL to request.
    Returns:
        The response body as text (HTML) if the request succeeds; otherwise `None`
        after all retries are exhausted.
    Side Effects:
        Sleeps between retries and prints an error message on permanent failure.
    """
    last = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = _session.get(url, timeout=REQUEST_TIMEOUT)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30)
                time.sleep(wait)
                continue

            r.raise_for_status()
            return r.text

        except requests.RequestException as e:
            last = e
            backoff = min(2 ** attempt, 20) + random.uniform(0, 1.0)
            time.sleep(backoff)

    print(f"[ERROR] Failed permanently: {url} -> {last}")
    return None


# -----------------------------
# Listing pages (Playwright)
# -----------------------------
def get_listing_links_playwright(start_url: str) -> List[str]:
    """
    Collect detail page URLs from paginated listing pages using Playwright.

    - Navigates listing pages: page 1 at `start_url`, then `f"{start_url}/{i}"`.
    - Extracts <a href="..."> links matching DETAIL_PAGE_PATTERN.
    - Returns unique absolute URLs (joined against BASE_URL).
    """
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

            soup = make_soup(page.content())
            page_links: List[str] = []

            for a in soup.select("a[href]"):
                href = (a.get("href") or "").strip()
                if DETAIL_PAGE_PATTERN.match(href):
                    page_links.append(urljoin(BASE_URL, href))

            # de-dupe while preserving order
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
def parse_listing_html(url: str, html: str) -> ListingEntry:
    """
    Parse a business listing HTML page and extract key contact fields.
    Args:
        url: Canonical URL of the listing page (stored on the returned entry).
        html: Raw HTML content of the listing page.
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

    name = _clean(soup.select_one("h1").get_text(" ", strip=True) if soup.select_one("h1") else "")

    email = ""
    phone = ""
    website = ""

    a_mail = soup.select_one("a[href^='mailto:']")
    if a_mail:
        email = _clean(a_mail["href"].replace("mailto:", ""))

    a_tel = soup.select_one("a[href^='tel:']")
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
    "UID", "Registry Number", "Credit Reference"
]

def load_or_create_workbook(path: str):
    """
    Load an existing Excel workbook from ``path`` or create a new one if it does not exist.

    If the workbook is newly created, the active worksheet is initialized with the
    column headers defined by the module-level ``XLSX_HEADERS`` constant.

    Args:
        path: Filesystem path to the ``.xlsx`` workbook.

    Returns:
        A tuple ``(wb, ws)`` where ``wb`` is the loaded/created ``openpyxl.Workbook``
        and ``ws`` is the active ``openpyxl.worksheet.worksheet.Worksheet``.

    Raises:
        Any exception raised by ``openpyxl.load_workbook`` other than ``FileNotFoundError``
        (e.g., invalid/corrupt file, permission errors).
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
    return {
        ws.cell(row=r, column=1).value
        for r in range(2, ws.max_row + 1)
        if isinstance(ws.cell(row=r, column=1).value, str)
    }

def append_row(ws, row: ListingEntry):
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
    ])


# -----------------------------
# Main
# -----------------------------
def main():
    """
    Orchestrate the scraping workflow and persist results to an Excel workbook.
    This function:
    - Loads (or creates) the output workbook/worksheet and initializes a thread lock.
    - Reads URLs already saved in the worksheet to enable resume behavior.
    - Collects listing URLs from `START_URL` and filters out those already processed.
    - Concurrently fetches and parses remaining listings using a thread pool.
    - Appends each parsed row to the worksheet in a thread-safe manner.
    - Periodically saves progress every 25 completed listings, and saves once more at the end.
    Side Effects:
    - Performs network I/O to discover and fetch listings.
    - Writes/updates `OUTPUT_XLSX` on disk.
    Dependencies:
    Relies on the following globals and helpers being defined:
    `OUTPUT_XLSX`, `START_URL`, `HTTP_THREADS`,
    `load_or_create_workbook`, `read_done_urls`, `get_listing_links_playwright`,
    `fetch_and_parse_listing`, `append_row`.
    Returns:
        None
    """
    wb, ws = load_or_create_workbook(OUTPUT_XLSX)
    lock = threading.Lock()

    done_urls = read_done_urls(ws)
    print(f"[INFO] Resume enabled ({len(done_urls)} entries already saved)")

    links = get_listing_links_playwright(START_URL)
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
    main()
