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

## bash
pip install requests beautifulsoup4 openpyxl playwright
playwright install chromium


## Configuration

All website-specific settings are located at the top of directory_scraper.py:

BASE_URL = "https://example-directory.tld"
START_URL = "https://example-directory.tld/listings/region"
OUTPUT_XLSX = "business_listings.xlsx"

| Variable      | Description                          |
| ------------- | ------------------------------------ |
| `BASE_URL`    | Base domain of the directory website |
| `START_URL`   | First listing page (page 1)          |
| `OUTPUT_XLSX` | Output Excel file name               |


## Scraping Behavior

HTTP_THREADS = 1
REQUEST_TIMEOUT = 25
MAX_RETRIES = 6
SLEEP_DETAIL = (0.15, 0.45)
MAX_LISTING_PAGES = 500

| Variable            | Purpose                                    |
| ------------------- | ------------------------------------------ |
| `HTTP_THREADS`      | Number of parallel detail page requests    |
| `REQUEST_TIMEOUT`   | Timeout per HTTP request (seconds)         |
| `MAX_RETRIES`       | Retry attempts for failed requests         |
| `SLEEP_DETAIL`      | Random delay between requests (politeness) |
| `MAX_LISTING_PAGES` | Safety limit for paginated listings        |


## Detail Page Pattern

DETAIL_PAGE_PATTERN = re.compile(r"^/[a-z0-9-]+_[A-Za-z0-9]+$")

This regular expression defines how detail page URLs are detected on listing pages.

You may need to adjust this pattern depending on the directory website.


## Parsing Logic

The function parse_listing_html() contains heuristic parsing rules for:

Business name

Address

Phone number

Email

Website

Since directory layouts vary, this function is intentionally simple and
meant to be adapted per target website.


## Usage

python directory_scraper.py

The scraper will:

    Collect detail page URLs from listing pages

    Fetch and parse each detail page

    Append results to an Excel file

    Automatically resume if the script is interrupted




## Output

The generated Excel file contains the following columns:

    URL

    Name

    Address

    Phone

    Email

    Website

Additional fields can be added easily in the data model (ListingEntry).




## Disclaimer

    This project is intended for educational and research purposes only.

    Before scraping any website, make sure to:

    Review the website’s Terms of Service

    Respect robots.txt

    Use reasonable request limits

The author assumes no responsibility for misuse of this software.