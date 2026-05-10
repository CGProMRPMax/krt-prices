#!/usr/bin/env python3
"""
Kulathinal Rubber Estate Management — daily price scraper.
Scrapes RSS4, RSS5, ISNR20, Latex(60%) from Rubber Board, Kottayam market.
Output: prices/prices.json (consumed by https://www.kulathinal.com)

Run schedule: daily 10:00 IST via GitHub Actions (.github/workflows/fetch_prices.yml).
Source page updates around 4 PM IST market close, so morning runs pull
the previous business day's closing rate — correct for next-day display.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

RUBBER_BOARD_URL = "https://rubberboard.gov.in/public"
OUTPUT_FILE      = "prices/prices.json"
GRADES           = {"RSS4": "rss4", "RSS5": "rss5",
                    "ISNR20": "isnr20", "Latex(60%)": "latex60"}
HEADERS          = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                  "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}
IST              = timezone(timedelta(hours=5, minutes=30))


def fetch_prices():
    """Scrape rubberboard.gov.in. Returns {grade_key: per_100kg_price} or None on failure."""
    try:
        r = requests.get(RUBBER_BOARD_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"ERROR: scrape failed: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    found = {}
    for table in soup.find_all("table"):
        text = table.get_text()
        if "Kottayam" not in text or "RSS" not in text:
            continue
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

    return found or None


def load_prev():
    """Read previous prices/prices.json so we can compute daily change. Returns {} on miss."""
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                d = json.load(f)
                return {k: v.get("per_100kg", 0) for k, v in d.get("prices", {}).items()}
        except Exception:
            pass
    return {}


def main():
    print(f"Fetching from {RUBBER_BOARD_URL}...")
    prev = load_prev()
    prices = fetch_prices()

    if not prices:
        # Fail loudly. Don't silently write hardcoded fallbacks — the bot's red
        # status in GitHub Actions is the only signal that the scrape broke.
        print("ERROR: no prices scraped and refusing to write stale fallbacks.", file=sys.stderr)
        if prev:
            print("Previous prices on disk are kept untouched.", file=sys.stderr)
        sys.exit(1)

    # Always stamp time in IST (the bug in the prior version stamped UTC as "IST")
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

    # Skip rewrite if nothing actually changed (avoids empty commits)
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
