import requests
from bs4 import BeautifulSoup
import json, os
from datetime import datetime, timezone

RUBBER_BOARD_URL = "https://rubberboard.gov.in/public"
OUTPUT_FILE = "prices/prices.json"
GRADES = {"RSS4":"rss4","RSS5":"rss5","ISNR20":"isnr20","Latex(60%)":"latex60"}
HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"}

def fetch_prices():
    try:
        r = requests.get(RUBBER_BOARD_URL, headers=HEADERS, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"ERROR: {e}"); return None
    soup = BeautifulSoup(r.text, "html.parser")
    found = {}
    for table in soup.find_all("table"):
        if "Kottayam" in table.get_text() and "RSS" in table.get_text():
            for row in table.find_all("tr"):
                cells = row.find_all(["td","th"])
                if len(cells) >= 2:
                    gc = cells[0].get_text(strip=True).replace(" ","").replace("(","").replace(")","").lower()
                    for gl, gk in GRADES.items():
                        gl2 = gl.replace(" ","").replace("(","").replace(")","").lower()
                        if gl2 in gc or gc in gl2:
                            for cell in cells[1:]:
                                raw = "".join(c for c in cell.get_text(strip=True) if c.isdigit() or c==".")
                                if raw and float(raw) > 100:
                                    found[gk] = float(raw)
                                    print(f"  {gl}: Rs.{raw}/100kg"); break
            break
    return found or None

def load_prev():
    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE) as f:
                d = json.load(f)
                return {k: v.get("per_100kg",0) for k,v in d.get("prices",{}).items()}
        except: pass
    return {}

def main():
    prev = load_prev()
    prices = fetch_prices() or prev or {"rss4":21700,"rss5":21200,"isnr20":18600,"latex60":15110}
    now = datetime.now(timezone.utc)
    grades = {}
    for k, v in prices.items():
        p = prev.get(k, v)
        grades[k] = {"per_kg":round(v/100,2),"per_100kg":v,
                     "change":"up" if v>p else "down" if v<p else "flat",
                     "diff":round(abs(v-p),1)}
    out = {"updated_at":now.strftime("%d %b %Y"),"updated_time":now.strftime("%I:%M %p IST"),
           "market":"Kottayam","source":"Rubber Board, Govt. of India",
           "source_url":"https://rubberboard.gov.in/public","prices":grades}
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE,"w") as f: json.dump(out, f, indent=2)
    print(f"Done! Saved to {OUTPUT_FILE}")

if __name__ == "__main__": main()
