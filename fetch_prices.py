#!/usr/bin/env python3
"""
Kulathinal Rubber Estate Management — daily price scraper (hardened v2).
Scrapes RSS4, RSS5, ISNR20, Latex(60%) from Rubber Board, Kottayam market.
Output: prices/prices.json (consumed by https://www.kulathinal.com)

Run schedule: daily 10:00 IST via GitHub Actions.

v2 changes (10-May-2026):
  - Full browser-like header set (Indian gov sites have started blocking
    bare requests UA with 403 via Cloudflare/WAF rules)
  - requests.Session with urllib3 Retry adapter (3 tries, exp backoff)
  - Session cookies preserved across redirects
  - Verbose diagnostics on failure (status, headers, response excerpt)
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

RUBBER_BOARD_URL = "https://rubberboard.gov.in/public"
OUTPUT_FILE      = "prices/prices.json"
GRADES           = {"RSS4": "rss4", "RSS5": "rss5",
                    "ISNR20": "isnr20", "Latex(60%)": "latex60"}
IST              = timezone(timedelta(hours=5, minutes=30))

# Full Chrome-on-Windows header set. Order and capitalization matters for
# some WAFs — don't reorder these without testing.
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
              "image/avif,image/webp,image/apng,*/*;q=0.8,"
              "application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-IN,en;q=0.9,en-GB;q=0.8,en-US;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}


def make_session():
    """Build a requests.Session with retry logic and browser-like headers."""
    sess = requests.Session()
    sess.headers.update(BROWSER_HEADERS)
    retry = Retry(
        total=3,
        backoff_factor=2,             # waits: 0s, 2s, 4s between retries
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess


def fetch_prices():
    """Scrape rubberboard.gov.in. Returns {grade_key: per_100kg_price} or None."""
    sess = make_session()
    try:
        print(f"GET {RUBBER_BOARD_URL}")
        r = sess.get(RUBBER_BOARD_URL, timeout=30, allow_redirects=True)
        print(f"  status: {r.status_code}, final url: {r.url}, "
              f"bytes: {len(r.content)}, server: {r.headers.get('Server', '?')}")
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP {e.response.status_code} from {RUBBER_BOARD_URL}",
              file=sys.stderr)
        print(f"  Response headers: {dict(e.response.headers)}", file=sys.stderr)
        print(f"  Response excerpt: {e.response.text[:500]}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"ERROR: scrape failed: {type(e).__name__}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    found = {}
    table_count = 0
    for table in soup.find_all("table"):
        text = table.get_text()
        if "Kottayam" not in text or "RSS" not in text:
            continue
        table_count += 1
        for row in table.find_all("tr"):
            cells = row.find_all(["td", "th"])
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True).replace(" ", "").replace("(", "").replace(")", "").lower()
            for grade_label, grade_key in GRADES.items():
                norm = grade_label.replace(" ", "").replace("(", "").replace(")", "").lower()
                if norm in label or label in norm:
                    for cell in cells[1:]:
                        raw = "".join(c for c in cell.get_text(strip=True) if c.isdigit() or c == ".")
                        if raw and float(raw) > 100:
                            found[grade_key] = float(raw)
                            print(f"  {grade_label}: Rs.{raw}/100kg")
                            break
        break  # first matching table is the daily price table

    if not found:
        print(f"ERROR: scraped page OK ({len(r.content)} bytes, "
              f"{table_count} candidate tables) but no prices parsed.",
              file=sys.stderr)
        print(f"  HTML excerpt: {r.text[:800]}", file=sys.stderr)
    return found or None


def load_prev():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                d = json.load(f)
                return {k: v.get("per_100kg", 0) for k, v in d.get("prices", {}).items()}
        except Exception:
            pass
    return {}


def main():
    prev = load_prev()
    prices = fetch_prices()

    if not prices:
        print("ERROR: no prices scraped and refusing to write stale fallbacks.",
              file=sys.stderr)
        if prev:
            print("Previous prices on disk are kept untouched.", file=sys.stderr)
        sys.exit(1)

    now_ist = datetime.now(IST)
    grades = {}
    for k, v in prices.items():
        p = prev.get(k, v)
        grades[k] = {
            "per_kg":    round(v / 100, 2),
            "per_100kg": v,
            "change":    "up" if v > p else "down" if v < p else "flat",
            "diff":      round(abs(v - p), 1),
        }

    out = {
        "updated_at":   now_ist.strftime("%d %b %Y"),
        "updated_time": now_ist.strftime("%I:%M %p IST").lstrip("0"),
        "market":       "Kottayam",
        "source":       "Rubber Board, Govt. of India",
        "source_url":   RUBBER_BOARD_URL,
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
    print(f"Done. Wrote {OUTPUT_FILE} at {out['updated_at']} {out['updated_time']}")


if __name__ == "__main__":
    main()
