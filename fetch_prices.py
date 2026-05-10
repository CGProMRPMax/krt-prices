#!/usr/bin/env python3
"""
Kulathinal Rubber Estate Management — daily price scraper (v3).
Scrapes RSS4, RSS5, ISNR20, Latex(60%) from the Kottayam market section.
Output: prices/prices.json (consumed by https://www.kulathinal.com)

Run schedule: daily 10:00 IST via GitHub Actions.

v3 changes (10-May-2026):
  - Switched source from rubberboard.gov.in/public to commoditymarketlive.com
    Reason: rubberboard.gov.in went JS-rendered some time after our last
    successful scrape (09-May), so requests-based scraping returns the page
    shell with zero data tables. commoditymarketlive.com is server-rendered
    HTML republishing the same Rubber Board, Kottayam market data — same
    source-of-truth, ready to scrape.
  - Parser now targets the "Kottayam Market" section heading and reads the
    table beneath it.
  - 'source' field still credits 'Rubber Board, Govt. of India' since
    commoditymarketlive is a downstream republisher of Rubber Board data.
"""
import json
import os
import re
import sys
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

DATA_URL    = "https://www.commoditymarketlive.com/rubber-price"
OUTPUT_FILE = "prices/prices.json"
IST         = timezone(timedelta(hours=5, minutes=30))

# Map the labels appearing in the Kottayam table -> our JSON keys.
# Labels come from <a> tags inside the Category cell, e.g. "RSS4", "Latex(60%)".
GRADE_LABELS = {
    "RSS4":       "rss4",
    "RSS5":       "rss5",
    "ISNR20":     "isnr20",
    "Latex(60%)": "latex60",
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


def make_session():
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def parse_money(s):
    """'₹25,500.00' -> 25500.0. Returns None on parse failure."""
    if not s:
        return None
    cleaned = re.sub(r"[^\d.]", "", s)
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def find_kottayam_table(soup):
    """Locate the <table> that follows the 'Kottayam Market' heading."""
    for h in soup.find_all(["h1", "h2", "h3", "h4"]):
        text = h.get_text(strip=True)
        if "Kottayam" in text and "Market" in text:
            tbl = h.find_next("table")
            if tbl:
                return tbl
    return None


def fetch_prices():
    """Returns ({grade_key: {'price', 'prev'}}, source_date) or (None, None)."""
    sess = make_session()
    try:
        print(f"GET {DATA_URL}")
        r = sess.get(DATA_URL, timeout=30, allow_redirects=True)
        print(f"  status: {r.status_code}, bytes: {len(r.content)}")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP {e.response.status_code} from {DATA_URL}", file=sys.stderr)
        print(f"  Response excerpt: {e.response.text[:400]}", file=sys.stderr)
        return None, None
    except Exception as e:
        print(f"ERROR: scrape failed: {type(e).__name__}: {e}", file=sys.stderr)
        return None, None

    soup = BeautifulSoup(r.text, "html.parser")
    table = find_kottayam_table(soup)
    if not table:
        print("ERROR: could not locate Kottayam table on the page.", file=sys.stderr)
        print(f"  HTML excerpt: {r.text[:600]}", file=sys.stderr)
        return None, None

    found = {}
    source_date = None
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 4:                       # header / spacer row
            continue
        date_text  = cells[0].get_text(strip=True)
        category   = cells[1].get_text(strip=True)   # 'RSS4', 'Latex(60%)' etc
        price_text = cells[2].get_text(strip=True)   # '₹25,500.00'
        prev_text  = cells[3].get_text(strip=True)   # '₹25,300.00'

        if category not in GRADE_LABELS:
            continue
        price = parse_money(price_text)
        prev  = parse_money(prev_text)
        if price is None:
            continue

        key = GRADE_LABELS[category]
        found[key] = {"price": price, "prev": prev if prev is not None else price}
        source_date = source_date or date_text
        print(f"  {category}: Rs.{price}/100kg (prev Rs.{prev})")

    if not found:
        print("ERROR: Kottayam table found but no rows matched our grades.", file=sys.stderr)
        return None, None

    return found, source_date


def load_prev_per100():
    """Return {key: per_100kg} from the previously-saved prices.json (for fallback diff)."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                d = json.load(f)
                return {k: v.get("per_100kg", 0) for k, v in d.get("prices", {}).items()}
        except Exception:
            pass
    return {}


def main():
    historical = load_prev_per100()       # for change tracking if source has no prev_text
    fetched, source_date = fetch_prices()

    if not fetched:
        print("ERROR: no prices scraped and refusing to write stale fallbacks.", file=sys.stderr)
        if historical:
            print("Previous prices on disk are kept untouched.", file=sys.stderr)
        sys.exit(1)

    now_ist = datetime.now(IST)
    grades = {}
    for k, info in fetched.items():
        cur  = info["price"]
        # Prefer the upstream's "prev price" column. Fallback to our own last-saved.
        prev = info["prev"] if info["prev"] != cur else historical.get(k, cur)
        grades[k] = {
            "per_kg":    round(cur / 100, 2),
            "per_100kg": cur,
            "change":    "up" if cur > prev else "down" if cur < prev else "flat",
            "diff":      round(abs(cur - prev), 1),
        }

    out = {
        "updated_at":   now_ist.strftime("%d %b %Y"),
        "updated_time": now_ist.strftime("%I:%M %p IST").lstrip("0"),
        "market":       "Kottayam",
        "source":       "Rubber Board, Govt. of India",
        "source_url":   "https://rubberboard.gov.in/public",
        "source_date":  source_date,            # YYYY-MM-DD from upstream
        "prices":       grades,
    }

    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                existing = json.load(f)
            if existing.get("prices") == out["prices"]:
                print("No price change since last run — skipping rewrite.")
                return
        except Exception:
            pass

    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(out, f, indent=2)
    print(f"Done. Wrote {OUTPUT_FILE} at {out['updated_at']} {out['updated_time']} "
          f"(source date: {source_date})")


if __name__ == "__main__":
    main()
