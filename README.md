# Business-Directory-Scraper
A generic Python scraper for business directory websites with paginated listings and detail pages. Supports JavaScript-rendered pages, resume-safe scraping, and Excel export.
# Business Directory Scraper

A generic and configurable Python scraper for business directory websites.

This project is designed to extract business listings from directory-style websites that provide
paginated overview pages and individual detail pages.  
It is intentionally kept generic so it can be adapted to different directory platforms.

---

## Features

- Playwright-based scraping for JavaScript-rendered listing pages
- Requests + BeautifulSoup for fast detail page parsing
- Resume support (already scraped URLs are skipped automatically)
- Rate limiting and retry handling
- Excel export using openpyxl
- Configurable and easy to adapt to other websites

---

## Requirements

- Python 3.9+
- Google Chromium (installed via Playwright)

### Python dependencies

```bash
pip install requests beautifulsoup4 openpyxl playwright
playwright install chromium


Configuration

All website-specific settings are located at the top of directory_scraper.py: