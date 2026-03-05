# Business Directory Scraper

A generic and configurable Python scraper for business directory websites.

This project extracts business listings from directory-style websites that provide
paginated overview pages and individual detail pages.  
It is intentionally kept generic so it can be adapted to different directory platforms
**without editing any Python code** — only `config.yaml` needs to be changed.

---

## How It Works

### Scraping Pipeline

```mermaid
flowchart TD
    A([config.yaml]) --> B[Load Config & CLI args]
    B --> C[Resume: load existing Excel]
    C --> D[["Playwright\n(headless Chromium)\nCollect listing URLs"]]
    D --> E{More pages?}
    E -- yes --> F[Navigate to next page]
    F --> G[Extract detail-page hrefs]
    G --> E
    E -- no --> H[Filter already-scraped URLs]
    H --> I[["Thread Pool\nhttp_threads workers"]]
    I --> J[["Requests + BeautifulSoup\nFetch & parse each detail page"]]
    J --> K[Clean & normalise data]
    K --> L{enrich_missing_data?}
    L -- yes --> M[DuckDuckGo search for missing fields]
    M --> N[(Excel .xlsx + optional CSV\nappend row)]
    L -- no --> N
    N --> O{every 25 rows?}
    O -- yes --> P[Save checkpoint]
    P --> I
    O -- no --> I
    I --> Q([Done ✓])
```

### Architecture Overview

```mermaid
flowchart LR
    subgraph inputs [" Inputs "]
        CFG[config.yaml]
        CLI[CLI args\n--config\n--log-level]
    end

    subgraph scraper [" Scraper "]
        direction TB
        PW["🌐 Playwright\nJS-rendered listing pages"]
        TP["⚡ Thread Pool\nParallel detail fetches"]
        RQ["🔍 Requests + BeautifulSoup\nDetail page parser"]
        CLN["🧹 Data Cleaning\nphone · email · website"]
        DDG["🔎 DuckDuckGo\n(optional enrichment)"]
        PW -- "detail-page URLs" --> TP
        TP --> RQ
        RQ --> CLN
        CLN -- "missing fields?" --> DDG
    end

    subgraph outputs [" Outputs "]
        XL["📊 Excel file\n(URL, Name, Address,\nPhone, Email, Website …\nScraped At, Missing Fields)"]
        CSV["📄 CSV file\n(optional)"]
        LOG["📋 Log file\n(optional)"]
    end

    WEB["🏢 Directory\nWebsite"] -- "paginated listing pages" --> PW
    WEB -- "business detail pages" --> RQ
    inputs --> scraper
    CLN -- "parsed entries" --> XL
    CLN -- "parsed entries" --> CSV
    scraper -- "structured log lines" --> LOG
```

---

## Features

- **External configuration** — all settings in a single `config.yaml` file; no Python editing required
- **Configurable CSS selectors** — override per-field selectors in `config.yaml` to match any site's HTML structure
- **Data cleaning & normalisation** — phone numbers, email addresses, and website URLs are automatically cleaned and validated
- **Missing-field tracking** — a `Missing Fields` column lists which contact fields are empty; a `Scraped At` timestamp is added to every row
- **Optional DuckDuckGo enrichment** — set `enrich_missing_data: true` to automatically search for missing email, phone, or website data
- **Flexible output** — export to Excel (`.xlsx`), CSV, or both
- **Structured logging** — timestamped live shell output with configurable log levels; optional log file
- **Playwright-based scraping** — handles JavaScript-rendered listing pages
- **Requests + BeautifulSoup** — fast, lightweight parsing for detail pages
- **Resume support** — already scraped URLs are automatically skipped
- **Rate limiting and retry handling** — respects `Retry-After` headers; exponential back-off
- **Excel export** — `openpyxl`-powered, progress-saved every 25 rows

---

## Requirements

- Python 3.9+
- Google Chromium (installed via Playwright)

---

## Setup

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Install the Chromium browser used by Playwright
playwright install chromium
```

---

## Configuration

All website-specific settings live in **`config.yaml`** — no need to edit the Python source.

```yaml
# Target website
base_url: "https://example-directory.tld"
start_url: "https://example-directory.tld/listings/region"

# Output file
output_xlsx: "business_listings.xlsx"

# Output format: "xlsx" | "csv" | "both"
output_format: "xlsx"

# Performance
http_threads: 1
request_timeout: 25
max_retries: 6
max_listing_pages: 500

# Politeness delay (seconds, randomised)
sleep_detail_min: 0.15
sleep_detail_max: 0.45

# URL pattern — adjust to match the target site's detail-page URL scheme
detail_page_pattern: "^/[a-z0-9-]+_[A-Za-z0-9]+$"

# CSS selectors — override per field; leave empty ("") to use built-in heuristics
selectors:
  name:    "h1"
  email:   "a[href^='mailto:']"
  phone:   "a[href^='tel:']"
  website: ""    # empty → first external http link
  address: ""    # empty → postal-code heuristic

# Enrich missing data via DuckDuckGo (requires: pip install duckduckgo-search)
enrich_missing_data: false

# Logging
log_level: "INFO"   # DEBUG | INFO | WARNING | ERROR
log_file: ""        # leave empty to log to console only
```

### Configuration reference

| Key                    | Description                                                    | Default                         |
| ---------------------- | -------------------------------------------------------------- | ------------------------------- |
| `base_url`             | Base domain of the directory website                           | `https://example-directory.tld` |
| `start_url`            | First listing page (page 1)                                    | —                               |
| `output_xlsx`          | Output Excel file name                                         | `business_listings.xlsx`        |
| `output_format`        | `"xlsx"`, `"csv"`, or `"both"`                                 | `"xlsx"`                        |
| `http_threads`         | Parallel detail-page requests (keep low to be polite)          | `1`                             |
| `request_timeout`      | HTTP request timeout in seconds                                | `25`                            |
| `max_retries`          | Retry attempts per failed request                              | `6`                             |
| `max_listing_pages`    | Safety cap for paginated listing pages                         | `500`                           |
| `sleep_detail_min`     | Minimum random delay between requests (seconds)                | `0.15`                          |
| `sleep_detail_max`     | Maximum random delay between requests (seconds)                | `0.45`                          |
| `detail_page_pattern`  | Regex that identifies detail-page hrefs on listing pages       | `^/[a-z0-9-]+_[A-Za-z0-9]+$`   |
| `selectors.name`       | CSS selector for the business name                             | `"h1"`                          |
| `selectors.email`      | CSS selector for the email link                                | `"a[href^='mailto:']"`          |
| `selectors.phone`      | CSS selector for the phone link                                | `"a[href^='tel:']"`             |
| `selectors.website`    | CSS selector for the website link; `""` → first external link  | `""`                            |
| `selectors.address`    | CSS selector for the address block; `""` → postal-code scan    | `""`                            |
| `enrich_missing_data`  | Search DuckDuckGo for missing email/phone/website              | `false`                         |
| `log_level`            | Console/file log verbosity                                     | `INFO`                          |
| `log_file`             | If set, log messages are also written to this file             | *(empty)*                       |

---

## Usage

```bash
# Run with the default config.yaml in the current directory
python Business-Directory-Scraper.py

# Use a custom config file
python Business-Directory-Scraper.py --config my_config.yaml

# Override the log level at runtime
python Business-Directory-Scraper.py --config config.yaml --log-level DEBUG
```

### CLI arguments

| Argument      | Description                                           | Default         |
| ------------- | ----------------------------------------------------- | --------------- |
| `--config`    | Path to the YAML configuration file                   | `config.yaml`   |
| `--log-level` | Override log level (`DEBUG`/`INFO`/`WARNING`/`ERROR`) | *(from config)* |

---

## Live shell output

The scraper prints structured, timestamped log lines while running:

```
[INFO    ] 10:42:01  Config loaded from: config.yaml
[INFO    ] 10:42:01  Output file: business_listings.xlsx
[INFO    ] 10:42:01  Output format: xlsx
[INFO    ] 10:42:01  Resume: 0 entries already saved
[INFO    ] 10:42:02  [LIST] Page 1 — loading https://example-directory.tld/listings/region
[INFO    ] 10:42:04  [LIST] Page 1: +48 new links (total: 48)
[INFO    ] 10:42:05  [LIST] Page 2 — loading https://example-directory.tld/listings/region/2
...
[INFO    ] 10:45:11  [12/48] Saved: Acme Plumbing GmbH
[INFO    ] 10:45:14  [13/48] Saved: Best Electric AG [missing: address, phone]
...
[INFO    ] 10:48:22  Done — 48 listings written to business_listings.xlsx
```

Entries with incomplete data are flagged in the log (`[missing: …]`) so you can
quickly spot which records may need manual review.

---

## Output

The generated Excel/CSV file contains the following columns:

| Column           | Description                                                         |
| ---------------- | ------------------------------------------------------------------- |
| URL              | Detail page URL                                                     |
| Name             | Business name (from configured selector or `<h1>`)                  |
| Address          | Street address and city                                             |
| Phone            | Normalised phone number                                             |
| Email            | Validated email address                                             |
| Website          | First external website URL                                          |
| UID              | *(optional — extend parser)*                                        |
| Registry Number  | *(optional — extend parser)*                                        |
| Credit Reference | *(optional — extend parser)*                                        |
| Scraped At       | UTC timestamp of when this entry was scraped (ISO 8601)             |
| Missing Fields   | Comma-separated list of core fields that were empty after scraping  |

---

## Adapting the Scraper

### 1. Set the target website

Edit the `base_url`, `start_url`, and `detail_page_pattern` in `config.yaml`:

```yaml
base_url: "https://www.my-business-directory.com"
start_url: "https://www.my-business-directory.com/companies/region"
detail_page_pattern: "^/company/[a-z0-9-]+-[0-9]+$"
```

### 2. Override CSS selectors

If the default heuristics don't match the target site's HTML, provide CSS selectors
in `config.yaml` — no Python editing required:

```yaml
selectors:
  name:    "h1.entry-title"
  email:   ".contact-block a[href^='mailto:']"
  phone:   ".phone-number"
  website: ".external-link"
  address: "address.company-address"
```

### 3. Enable enrichment for missing data

Install the optional search library and enable the feature:

```bash
pip install duckduckgo-search
```

```yaml
enrich_missing_data: true
```

When enabled, any entry with a missing email, phone, or website will trigger a
DuckDuckGo web search using the business name and address. Matched values are
cleaned and written to the entry before it is saved.

### 4. Choose an output format

```yaml
output_format: "both"   # writes both business_listings.xlsx and business_listings.csv
```

### 5. Tune performance

Increase `http_threads` for faster scraping, or raise `sleep_detail_min`/`sleep_detail_max`
to be more polite to the target server.

---

## Disclaimer

This project is intended for educational and research purposes only.

Before scraping any website, make sure to:

- Review the website's Terms of Service
- Respect `robots.txt`
- Use reasonable request limits

The author assumes no responsibility for misuse of this software.
