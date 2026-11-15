#!/usr/bin/env python3
"""
Stellt das Aktien-Universum zusammen: Fragt den Yahoo-Aktienscreener nach allen
Unternehmen ab einer Mindestgröße (Börsenwert) an den Heimatbörsen der gewählten
Länder ab und schreibt pipeline/universe.csv. Zweitlistings (z. B. Apple an der
Frankfurter Börse) werden aussortiert.

Einstellungen: MIN_MARKET_CAP_EUR und MARKETS unten.
Zusätzliche Wunsch-Aktien (auch kleinere) gehören in pipeline/tickers.csv.
"""
import csv
import re
import sys
import time
from pathlib import Path

import yfinance as yf
from yfinance import EquityQuery as Q

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "universe.csv"

# Mindest-Börsenwert in Euro. 10 Mrd. ergibt rund 1.300 Unternehmen weltweit.
MIN_MARKET_CAP_EUR = float(sys.argv[1]) if (__name__ == "__main__" and len(sys.argv) > 1) else 10e9

# Yahoo-Region -> Heimatbörsen. Reihenfolge = Vorrang bei Doppel-Listings
# (Nicht-US-Märkte zuerst, damit z. B. Novartis in Zürich statt als US-ADR geführt wird).
MARKETS = [
    ("de", ["GER"]), ("ch", ["EBS"]), ("at", ["VIE"]),
    ("gb", ["LSE"]), ("fr", ["PAR"]), ("nl", ["AMS"]), ("be", ["BRU"]), ("es", ["MCE"]),
    ("it", ["MIL"]), ("se", ["STO"]), ("dk", ["CPH"]), ("no", ["OSL"]), ("fi", ["HEL"]),
    ("ie", ["ISE"]), ("pt", ["LIS"]),
    ("jp", ["JPX"]), ("au", ["ASX"]), ("hk", ["HKG"]), ("sg", ["SES"]), ("tw", ["TAI"]), ("kr", ["KSC"]),
    ("ca", ["TOR"]),
    ("us", ["NMS", "NYQ"]),
]
# Märkte, an denen viele Heimat-Unternehmen in US-Dollar bilanzieren (Shell, Novartis, BHP …)
USD_REPORTING_OK = {"gb", "ch", "au", "hk", "nl", "dk", "no", "sg", "ie"}
MINOR = {"GBp": "GBP", "GBX": "GBP", "ZAc": "ZAR", "ILA": "ILS"}

_fx = {}


def eur_to(ccy):
    """Wie viele Einheiten der Währung entsprechen 1 EUR."""
    ccy = MINOR.get(ccy, ccy)
    if ccy == "EUR":
        return 1.0
    if ccy not in _fx:
        h = yf.Ticker(f"EUR{ccy}=X").history(period="5d")
        _fx[ccy] = float(h["Close"].iloc[-1])
    return _fx[ccy] * (100 if ccy == "GBP" else 1)  # Yahoo-Börsenwert in GBp


def norm_name(n):
    n = (n or "").lower()
    n = re.sub(r"[^a-z0-9 ]", " ", n)
    n = re.sub(r"\b(inc|corp|corporation|co|ltd|limited|plc|ag|se|sa|nv|n v|asa|ab|oyj|spa|s p a|holding|holdings|group|the|company|kgaa)\b", " ", n)
    return " ".join(n.split())


def screen(region, exchanges, cur_ccy_hint):
    out, offset = [], 0
    while True:
        conds = [Q("eq", ["region", region])]
        conds.append(Q("is-in", ["exchange", *exchanges]) if len(exchanges) > 1 else Q("eq", ["exchange", exchanges[0]]))
        q = Q("and", conds + [Q("gt", ["intradaymarketcap", MIN_MARKET_CAP_EUR * cur_ccy_hint])])
        for attempt in range(4):
            try:
                r = yf.screen(q, offset=offset, size=250, sortField="intradaymarketcap", sortAsc=False)
                break
            except Exception as e:
                print("  Wiederhole:", e)
                time.sleep(5 * (attempt + 1))
        else:
            return out
        quotes = r.get("quotes", [])
        out += quotes
        offset += len(quotes)
        if not quotes or offset >= (r.get("total") or 0):
            return out


# Währung je Region, um die Größenschwelle umzurechnen
REGION_CCY = {"de": "EUR", "ch": "CHF", "at": "EUR", "gb": "GBp", "fr": "EUR", "nl": "EUR", "be": "EUR",
              "es": "EUR", "it": "EUR", "se": "SEK", "dk": "DKK", "no": "NOK", "fi": "EUR", "ie": "EUR",
              "pt": "EUR", "jp": "JPY", "au": "AUD", "hk": "HKD", "sg": "SGD", "tw": "TWD", "kr": "KRW",
              "ca": "CAD", "us": "USD"}


EXCHANGE_COUNTRY = {
    "GER": {"Germany"}, "EBS": {"Switzerland"}, "VIE": {"Austria"},
    "LSE": {"United Kingdom", "Jersey", "Guernsey", "Isle of Man"}, "PAR": {"France"},
    "AMS": {"Netherlands"}, "BRU": {"Belgium"}, "MCE": {"Spain"}, "MIL": {"Italy"},
    "STO": {"Sweden"}, "CPH": {"Denmark"}, "OSL": {"Norway"}, "HEL": {"Finland"},
    "ISE": {"Ireland"}, "LIS": {"Portugal"}, "JPX": {"Japan"}, "ASX": {"Australia"},
    "HKG": {"Hong Kong", "China", "Macau"}, "SES": {"Singapore"}, "TAI": {"Taiwan"},
    "KSC": {"South Korea"}, "TOR": {"Canada"}, "NMS": {"United States"}, "NYQ": {"United States"},
}


def country_of(sym):
    for attempt in range(4):
        try:
            c = (yf.Ticker(sym).info or {}).get("country")
            if c:
                return c
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return None


def main():
    from concurrent.futures import ThreadPoolExecutor
    groups = {}  # Name -> Liste von Listings in Vorrang-Reihenfolge
    for region, exch in MARKETS:
        quotes = screen(region, exch, eur_to(REGION_CCY[region]))
        n = 0
        for x in quotes:
            if x.get("quoteType") != "EQUITY":
                continue
            key = norm_name(x.get("longName") or x.get("shortName"))
            if not key:
                continue
            groups.setdefault(key, []).append(x)
            n += 1
        print(f"{region}: {n} Listings", flush=True)
    print(f"{len(groups)} Unternehmen (vor Heimatbörsen-Abgleich), lade Firmensitz …", flush=True)

    keys = list(groups)
    with ThreadPoolExecutor(max_workers=8) as ex:
        def lookup(k):
            for x in groups[k][:3]:
                c = country_of(x["symbol"])
                if c:
                    return c
            return None
        countries = dict(zip(keys, ex.map(lookup, keys)))

    rows = []
    for k in keys:
        listings, country = groups[k], countries.get(k)
        if not country:
            continue
        home = [x for x in listings if country in EXCHANGE_COUNTRY.get(x.get("exchange"), set())]
        if home:
            # Bei mehreren Aktiengattungen an der Heimatbörse die meistgehandelte nehmen
            ex0 = home[0].get("exchange")
            same = [x for x in home if x.get("exchange") == ex0]
            pick = max(same, key=lambda x: (x.get("averageDailyVolume3Month") or 0) * (x.get("regularMarketPrice") or 0))
        else:
            # Kein Heimatlisting gefunden (z. B. irische Firmen wie Linde, die in den USA notieren):
            # dann das US-Listing nehmen. Sonst ist es ein Zweitlisting -> weglassen.
            us = [x for x in listings if x.get("exchange") in ("NMS", "NYQ")]
            if not us:
                continue
            pick = max(us, key=lambda x: (x.get("averageDailyVolume3Month") or 0) * (x.get("regularMarketPrice") or 0))
        rows.append({"ticker": pick["symbol"], "name": pick.get("longName") or pick.get("shortName"), "country": country})

    with open(OUT, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["ticker", "name", "country"])
        w.writeheader()
        w.writerows(rows)
    print(f"{len(rows)} Unternehmen in {OUT}")


if __name__ == "__main__":
    main()
