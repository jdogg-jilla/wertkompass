"""
Lange Bilanz-Historie aus den kostenlosen XBRL-Daten der US-Börsenaufsicht SEC.

- US-Unternehmen (10-K): maschinenlesbar seit ca. 2009
- Ausländische Konzerne mit US-Notiz (20-F/40-F, z. B. SAP, Novartis, Toyota): meist seit ca. 2018

Die SEC verlangt eine Kontaktangabe im User-Agent. Sie wird über die Umgebungsvariable
SEC_USER_AGENT gesetzt (z. B. "Wertkompass name@example.com"). Ohne sie bleibt die SEC aus.
"""
import os
import threading
import time

import requests

UA = os.environ.get("SEC_USER_AGENT", "").strip()
ENABLED = bool(UA)
_session = requests.Session()
_lock = threading.Lock()
_last = [0.0]
_tickers = None
_tlock = threading.Lock()

ANNUAL_FORMS = {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A", "10-KT"}

# Kennzahl -> mögliche XBRL-Bezeichnungen (in Vorrang-Reihenfolge), getrennt nach Rechnungslegung
CONCEPTS = {
    "us-gaap": {
        "revenue": ["Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax", "SalesRevenueNet",
                    "RevenueFromContractWithCustomerIncludingAssessedTax", "SalesRevenueGoodsNet", "RevenuesNetOfInterestExpense"],
        "netIncome": ["NetIncomeLoss", "NetIncomeLossAvailableToCommonStockholdersBasic", "ProfitLoss"],
        "eps": ["EarningsPerShareDiluted", "EarningsPerShareBasicAndDiluted", "EarningsPerShareBasic"],
        "opIncome": ["OperatingIncomeLoss"],
        "ocf": ["NetCashProvidedByUsedInOperatingActivities", "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"],
        "capex": ["PaymentsToAcquirePropertyPlantAndEquipment", "PaymentsToAcquireProductiveAssets"],
        "shares": ["WeightedAverageNumberOfDilutedSharesOutstanding", "WeightedAverageNumberOfSharesOutstandingBasicAndDiluted"],
    },
    "ifrs-full": {
        "revenue": ["Revenue", "RevenueFromContractsWithCustomers"],
        "netIncome": ["ProfitLossAttributableToOwnersOfParent", "ProfitLoss"],
        "eps": ["DilutedEarningsLossPerShare", "BasicAndDilutedEarningsLossPerShare", "BasicEarningsLossPerShare"],
        "opIncome": ["ProfitLossFromOperatingActivities"],
        "ocf": ["CashFlowsFromUsedInOperatingActivities"],
        "capex": ["PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities", "PurchaseOfPropertyPlantAndEquipment"],
        "shares": ["AdjustedWeightedAverageShares", "WeightedAverageShares"],
    },
}


def _get(url):
    """Höflicher Abruf: höchstens ~6 Anfragen pro Sekunde (SEC-Grenze: 10)."""
    for attempt in range(4):
        with _lock:
            wait = 0.17 - (time.time() - _last[0])
            if wait > 0:
                time.sleep(wait)
            _last[0] = time.time()
        try:
            r = _session.get(url, headers={"User-Agent": UA, "Accept-Encoding": "gzip"}, timeout=30)
            if r.status_code == 404:
                return None
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep(2 * (attempt + 1))
    return None


def tickers():
    """SEC-Liste: Börsenkürzel und Firmenname -> CIK-Nummer."""
    global _tickers
    with _tlock:
        if _tickers is None:
            data = _get("https://www.sec.gov/files/company_tickers.json") or {}
            by_ticker, by_name = {}, {}
            for x in data.values():
                by_ticker[x["ticker"].upper()] = x["cik_str"]
                by_name.setdefault(x["title"], x["cik_str"])
            _tickers = (by_ticker, by_name)
    return _tickers


def find_cik(symbol, name, norm_name):
    if not ENABLED:
        return None
    by_ticker, by_name = tickers()
    if "." not in symbol:  # US-Listing
        return by_ticker.get(symbol.upper())
    # Heimatlisting im Ausland: über den Firmennamen suchen (wird später mit Yahoo-Umsätzen geprüft)
    key = norm_name(name)
    if not key:
        return None
    for title, cik in by_name.items():
        if norm_name(title) == key:
            return cik
    return None


def annual_history(cik):
    """Gibt {Bilanzstichtag: {kennzahl: wert, 'filed': datum}} und die Berichtswährung zurück."""
    facts = _get(f"https://data.sec.gov/api/xbrl/companyfacts/CIK{int(cik):010d}.json")
    if not facts or "facts" not in facts:
        return {}, None
    taxo = "us-gaap" if "us-gaap" in facts["facts"] and "ifrs-full" not in facts["facts"] else (
        "ifrs-full" if "ifrs-full" in facts["facts"] else "us-gaap")
    f = facts["facts"].get(taxo, {})
    rows, currency = {}, None
    for key, names in CONCEPTS[taxo].items():
        for concept in names:
            if concept not in f:
                continue
            for unit, items in f[concept]["units"].items():
                if key not in ("eps", "shares"):
                    if "/" in unit or unit == "shares":
                        continue
                    currency = currency or unit
                    if unit != currency:
                        continue
                for x in items:
                    if x.get("form") not in ANNUAL_FORMS or "start" not in x:
                        continue
                    days = (_d(x["end"]) - _d(x["start"])) / 86400
                    if not 330 <= days <= 400:
                        continue
                    row = rows.setdefault(x["end"], {})
                    prev = row.get(key)
                    # Bei mehreren Werten zählt die höher priorisierte Bezeichnung, dann die neueste Meldung
                    rank = names.index(concept)
                    if prev is None or rank < prev[2] or (rank == prev[2] and x["filed"] > prev[1]):
                        row[key] = (x["val"], x["filed"], rank)
    out = {}
    for end, row in rows.items():
        out[end] = {k: v[0] for k, v in row.items()}
        out[end]["filed"] = {k: v[1] for k, v in row.items()}
    return out, currency


def _d(s):
    return time.mktime(time.strptime(s, "%Y-%m-%d"))
