#!/usr/bin/env python3
"""
Lädt Kurs-, Bilanz- und Dividendendaten von Yahoo Finance für alle Aktien in
tickers.csv, berechnet Kennzahlen, Scores und faire Werte und schreibt das
Ergebnis nach data/stocks.json. Die Web-App liest nur diese Datei.

Aufruf:  python pipeline/fetch.py            (alle Aktien)
         python pipeline/fetch.py MSFT SAP.DE (nur einzelne, zum Testen)
"""
import csv
import datetime as dt
import json
import math
import sys
import threading
import time
import traceback
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sec  # noqa: E402
from universe import norm_name  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "data" / "stocks.json"

# --------------------------------------------------------------------------
# Zuordnungen
# --------------------------------------------------------------------------
REGION = {
    "United States": "Nordamerika", "Canada": "Nordamerika",
    "Germany": "DACH", "Switzerland": "DACH", "Austria": "DACH",
    "United Kingdom": "Europa", "France": "Europa", "Netherlands": "Europa",
    "Denmark": "Europa", "Sweden": "Europa", "Norway": "Europa", "Finland": "Europa",
    "Spain": "Europa", "Italy": "Europa", "Belgium": "Europa", "Ireland": "Europa",
    "Jersey": "Europa", "Luxembourg": "Europa", "Portugal": "Europa",
    "Japan": "Asien-Pazifik", "Australia": "Asien-Pazifik", "Hong Kong": "Asien-Pazifik",
    "China": "Asien-Pazifik", "Taiwan": "Asien-Pazifik", "Singapore": "Asien-Pazifik",
    "South Korea": "Asien-Pazifik", "New Zealand": "Asien-Pazifik", "Macau": "Asien-Pazifik",
    "Bermuda": "Nordamerika", "Guernsey": "Europa", "Isle of Man": "Europa",
    "Brazil": "Schwellenländer", "India": "Schwellenländer", "Israel": "Schwellenländer", "Chile": "Schwellenländer",
    "Mexico": "Schwellenländer", "South Africa": "Schwellenländer", "Kazakhstan": "Schwellenländer",
    "Uruguay": "Schwellenländer", "Peru": "Schwellenländer", "Indonesia": "Schwellenländer",
    "Colombia": "Schwellenländer", "Argentina": "Schwellenländer",
}
COUNTRY_DE = {
    "United States": "USA", "Canada": "Kanada", "Germany": "Deutschland",
    "Switzerland": "Schweiz", "Austria": "Österreich", "United Kingdom": "Großbritannien",
    "France": "Frankreich", "Netherlands": "Niederlande", "Denmark": "Dänemark",
    "Sweden": "Schweden", "Norway": "Norwegen", "Finland": "Finnland", "Spain": "Spanien",
    "Italy": "Italien", "Belgium": "Belgien", "Ireland": "Irland", "Jersey": "Jersey",
    "Japan": "Japan", "Australia": "Australien", "Hong Kong": "Hongkong", "China": "China",
    "Taiwan": "Taiwan", "Singapore": "Singapur", "South Korea": "Südkorea",
    "Brazil": "Brasilien", "Bermuda": "Bermuda", "India": "Indien", "Israel": "Israel", "Chile": "Chile",
    "Mexico": "Mexiko", "Cayman Islands": "Kaimaninseln", "South Africa": "Südafrika", "Kazakhstan": "Kasachstan",
    "Uruguay": "Uruguay", "Peru": "Peru", "Indonesia": "Indonesien", "Colombia": "Kolumbien", "Argentina": "Argentinien",
    "Luxembourg": "Luxemburg", "Portugal": "Portugal", "Macau": "Macau", "Guernsey": "Guernsey", "Isle of Man": "Isle of Man",
}
SECTOR_DE = {
    "Technology": "Technologie", "Healthcare": "Gesundheit",
    "Consumer Defensive": "Basiskonsumgüter", "Consumer Cyclical": "Zyklischer Konsum",
    "Industrials": "Industrie", "Financial Services": "Finanzen",
    "Communication Services": "Kommunikation", "Basic Materials": "Grundstoffe",
    "Energy": "Energie", "Utilities": "Versorger", "Real Estate": "Immobilien",
}
# Kurse in Pence/Cent werden auf die Hauptwährung umgerechnet
MINOR_CCY = {"GBp": ("GBP", 0.01), "GBX": ("GBP", 0.01), "ZAc": ("ZAR", 0.01), "ILA": ("ILS", 0.01)}

# --------------------------------------------------------------------------
# Hilfsfunktionen
# --------------------------------------------------------------------------
_fx_cache: dict = {}
_fx_lock = threading.Lock()


def fx_series(frm: str, to: str):
    """Tägliche Wechselkurse frm->to der letzten 12 Jahre (oder None bei gleicher Währung)."""
    if not frm or not to or frm == to:
        return None
    key = (frm, to)
    with _fx_lock:
        if key in _fx_cache:
            return _fx_cache[key]
    s = None
    for attempt in range(3):
        try:
            h = yf.Ticker(f"{frm}{to}=X").history(period="12y", interval="1d", auto_adjust=False)
            if len(h):
                s = h["Close"].copy()
                s.index = s.index.tz_localize(None)
                break
        except Exception:
            time.sleep(2)
    with _fx_lock:
        _fx_cache[key] = s
    return s


def fx_at(frm, to, date=None) -> float:
    if not frm or not to or frm == to:
        return 1.0
    s = fx_series(frm, to)
    if s is None or s.empty:
        raise RuntimeError(f"Kein Wechselkurs {frm}->{to}")
    if date is None:
        return float(s.iloc[-1])
    d = pd.Timestamp(date).tz_localize(None) if pd.Timestamp(date).tzinfo else pd.Timestamp(date)
    sub = s[s.index <= d]
    return float(sub.iloc[-1] if len(sub) else s.iloc[0])


def num(x):
    try:
        x = float(x)
    except (TypeError, ValueError):
        return None
    return None if (math.isnan(x) or math.isinf(x)) else x


def row(df: pd.DataFrame, *names) -> pd.Series:
    """Erste vorhandene Zeile einer Yahoo-Tabelle als Serie {Datum: Wert}, aufsteigend sortiert."""
    if df is None or df.empty:
        return pd.Series(dtype=float)
    for n in names:
        if n in df.index:
            s = pd.to_numeric(df.loc[n], errors="coerce")
            s.index = pd.to_datetime(s.index)
            return s.sort_index()
    return pd.Series(dtype=float)


def last(s: pd.Series):
    s = s.dropna()
    return num(s.iloc[-1]) if len(s) else None


def cagr(first, last_, years):
    if first is None or last_ is None or years <= 0 or first <= 0 or last_ <= 0:
        return None
    return (last_ / first) ** (1 / years) - 1


def lin(x, lo, hi):
    """Lineare Skala: lo -> 0, hi -> 1, begrenzt."""
    if x is None:
        return None
    if hi == lo:
        return 1.0 if x >= hi else 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def weighted(parts):
    """parts: [(score 0..1 or None, gewicht)] -> 0..100 (nur vorhandene Werte)."""
    tot = sum(w for s, w in parts if s is not None)
    if tot == 0:
        return None
    return round(100 * sum(s * w for s, w in parts if s is not None) / tot)


def r_squared(values):
    y = np.array([v for v in values if v is not None], dtype=float)
    if len(y) < 3:
        return None
    x = np.arange(len(y))
    slope, inter = np.polyfit(x, y, 1)
    pred = slope * x + inter
    ss_res = ((y - pred) ** 2).sum()
    ss_tot = ((y - y.mean()) ** 2).sum()
    r2 = 1.0 if ss_tot == 0 else 1 - ss_res / ss_tot
    # Fallende Gewinne sind keine "stabile" Entwicklung
    return max(0.0, r2) * (1.0 if slope >= 0 else 0.0)


def clean(o):
    """NaN/Inf -> None und Rundung, damit gültiges, kompaktes JSON entsteht."""
    if isinstance(o, dict):
        return {k: clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [clean(v) for v in o]
    if isinstance(o, (float, np.floating)):
        f = float(o)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, 6) if abs(f) < 1 else round(f, 4)
    if isinstance(o, (np.integer,)):
        return int(o)
    return o


# --------------------------------------------------------------------------
# Analyse einer Aktie
# --------------------------------------------------------------------------
def analyse(symbol: str, prev: dict | None = None) -> dict:
    t = yf.Ticker(symbol)
    info = t.info or {}
    raw_ccy = info.get("currency")
    if not raw_ccy:
        raise RuntimeError("keine Kursdaten")
    ccy, unit = MINOR_CCY.get(raw_ccy, (raw_ccy, 1.0))
    fin_ccy = info.get("financialCurrency") or ccy

    price = num(info.get("currentPrice")) or num(info.get("regularMarketPrice"))
    if not price:
        raise RuntimeError("kein Kurs")
    price *= unit

    # ---- Kurshistorie (monatlich, 20 Jahre) ----------------------------
    hist = t.history(period="20y", interval="1mo", auto_adjust=False)
    hist = hist[hist["Close"].notna()]
    closes = hist["Close"] * unit
    closes.index = closes.index.tz_localize(None)
    price_hist = [[d.strftime("%Y-%m"), round(float(v), 4)] for d, v in closes.items()]

    def price_at(date):
        sub = closes[closes.index <= pd.Timestamp(date)]
        return float(sub.iloc[-1]) if len(sub) else None

    # ---- Jahresabschlüsse ---------------------------------------------
    inc, cf, bs = t.income_stmt, t.cashflow, t.balance_sheet
    revenue = row(inc, "Total Revenue", "Operating Revenue")
    net_income = row(inc, "Net Income Common Stockholders", "Net Income")
    eps = row(inc, "Diluted EPS", "Basic EPS")
    op_income = row(inc, "Operating Income", "EBIT")
    ebitda = row(inc, "EBITDA", "Normalized EBITDA")
    shares = row(inc, "Diluted Average Shares", "Basic Average Shares")
    ocf = row(cf, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
    capex = row(cf, "Capital Expenditure")
    fcf = row(cf, "Free Cash Flow")
    div_paid = row(cf, "Cash Dividends Paid", "Common Stock Dividend Paid")
    equity = row(bs, "Stockholders Equity", "Common Stock Equity")
    assets = row(bs, "Total Assets")
    debt = row(bs, "Total Debt")
    cash = row(bs, "Cash Cash Equivalents And Short Term Investments", "Cash And Cash Equivalents")

    if fcf.dropna().empty and not ocf.dropna().empty and not capex.dropna().empty:
        fcf = ocf.add(capex, fill_value=0)
    if eps.dropna().empty and not net_income.empty and not shares.empty:
        eps = net_income / shares

    # ---- Lange Historie zusammenführen ----------------------------------
    # Rohdaten je Bilanzstichtag (in Berichtswährung). Vorrang: Yahoo (aktuell) > SEC > Archiv.
    raw = {}

    def put(end, vals, ccy_, src):
        end = pd.Timestamp(end)
        for k in list(raw):  # gleiche Geschäftsjahre mit leicht abweichendem Stichtag zusammenlegen
            if abs((pd.Timestamp(k) - end).days) <= 20:
                end = pd.Timestamp(k)
                break
        key = end.strftime("%Y-%m-%d")
        cur = raw.setdefault(key, {"ccy": ccy_, "src": src})
        if cur.get("ccy") != ccy_ and any(cur.get(x) is not None for x in ("revenue", "eps")):
            return
        cur["ccy"] = ccy_
        for k, v in vals.items():
            if v is not None and not (isinstance(v, float) and math.isnan(v)):
                cur[k] = v
        cur["src"] = src

    for r0 in (prev or {}).get("annualRaw", []):  # Archiv: früher gespeicherte Jahre
        put(r0["date"], {k: r0.get(k) for k in ("revenue", "netIncome", "eps", "opIncome", "ocf", "fcf", "shares")}, r0["ccy"], r0.get("src", "archiv"))

    sec_years, sec_used = 0, False
    try:
        cik = sec.find_cik(symbol, info.get("longName") or info.get("shortName"), norm_name) if sec.ENABLED else None
        if cik:
            hist_sec, sec_ccy = sec.annual_history(cik)
            splits = t.splits
            if splits is not None and len(splits):
                splits.index = splits.index.tz_localize(None)
            # Plausibilitätsprüfung: Umsatz muss in überlappenden Jahren zu Yahoo passen (falsche Zuordnung vermeiden)
            checks = []
            for d0, yv in revenue.dropna().items():
                for end, srow in hist_sec.items():
                    if abs((pd.Timestamp(end) - d0).days) <= 20 and srow.get("revenue"):
                        f0 = fx_at(sec_ccy, fin_ccy, d0) if sec_ccy != fin_ccy else 1.0
                        checks.append(abs(srow["revenue"] * f0 / yv - 1) < 0.06)
            if hist_sec and sec_ccy and (not checks or sum(checks) >= max(1, len(checks) // 2)) and (checks or "." not in symbol):
                for end, srow in sorted(hist_sec.items()):
                    vals = {k: srow.get(k) for k in ("revenue", "netIncome", "eps", "opIncome", "ocf", "shares")}
                    # Aktiensplits nach der Meldung herausrechnen (alte Meldungen sind nicht angepasst)
                    filed = srow["filed"].get("eps") or srow["filed"].get("shares")
                    if filed and splits is not None and len(splits):
                        fac = float(np.prod([v for dd, v in splits.items() if dd > pd.Timestamp(filed)] or [1.0]))
                        if vals.get("eps") is not None:
                            vals["eps"] = vals["eps"] / fac
                        if vals.get("shares") is not None:
                            vals["shares"] = vals["shares"] * fac
                    if srow.get("ocf") is not None and srow.get("capex") is not None:
                        vals["fcf"] = srow["ocf"] - abs(srow["capex"])
                    put(end, vals, sec_ccy, "SEC")
                sec_years, sec_used = len(hist_sec), True
                # Gewinn je Aktie abgleichen: andere Aktiengattung/ADR-Verhältnis -> SEC-EPS verwerfen
                ratios = []
                for d0, yv in eps.dropna().items():
                    for k, rw in raw.items():
                        if rw.get("src") == "SEC" and abs((pd.Timestamp(k) - d0).days) <= 20 and rw.get("eps") and yv:
                            ratios.append(rw["eps"] / yv)
                if ratios and not (0.8 < float(np.median(ratios)) < 1.25):
                    for rw in raw.values():
                        if rw.get("src") == "SEC":
                            rw["eps"] = None
                            rw["shares"] = None
    except Exception as e:  # SEC ist ein Zusatz: Fehler dürfen den Rest nicht stoppen
        print(f"   SEC-Hinweis {symbol}: {e}", flush=True)

    for d0 in sorted(set(revenue.dropna().index) | set(eps.dropna().index)):
        put(d0, {"revenue": num(revenue.get(d0)), "netIncome": num(net_income.get(d0)), "eps": num(eps.get(d0)),
                 "opIncome": num(op_income.get(d0)), "ocf": num(ocf.get(d0)), "fcf": num(fcf.get(d0)),
                 "shares": num(shares.get(d0))}, fin_ccy, "Yahoo")

    annual_raw = []
    for key in sorted(raw):
        rw = raw[key]
        if rw.get("revenue") is None and rw.get("eps") is None:
            continue
        annual_raw.append({"date": key, **{k: rw.get(k) for k in ("revenue", "netIncome", "eps", "opIncome", "ocf", "fcf", "shares")},
                           "ccy": rw["ccy"], "src": rw.get("src")})
    annual_raw = annual_raw[-20:]

    first_price = closes.index.min() if len(closes) else None
    annual = []
    for a in annual_raw:
        d = pd.Timestamp(a["date"])
        f = fx_at(a["ccy"], ccy, d)
        sh, e, o, fc = a["shares"], a["eps"], a["ocf"], a["fcf"]
        p = price_at(d) if (first_price is not None and d >= first_price) else None
        e_p = e * f if e is not None else None
        ocf_ps = o / sh * f if (o is not None and sh) else None
        fcf_ps = fc / sh * f if (fc is not None and sh) else None
        rev = a["revenue"] * fx_at(a["ccy"], fin_ccy, d) if (a["revenue"] is not None and a["ccy"] != fin_ccy) else a["revenue"]
        annual.append({
            "year": d.year, "date": a["date"], "src": a["src"],
            "revenue": rev, "netIncome": a["netIncome"], "opIncome": a["opIncome"], "ocf": o, "fcf": fc,
            "eps": e_p, "ocfPerShare": ocf_ps, "fcfPerShare": fcf_ps, "price": p,
            "pe": (p / e_p) if (p and e_p and e_p > 0) else None,
            "pcf": (p / ocf_ps) if (p and ocf_ps and ocf_ps > 0) else None,
        })
    # Für Kennzahlen höchstens die letzten 10 Geschäftsjahre (wie bei Aktienfinder üblich)
    annual10 = annual[-10:]

    # TTM-Gewinn je Aktie (letzte 12 Monate), in Kurswährung
    eps_ttm = None
    try:
        ttm = t.ttm_income_stmt
        e = last(row(ttm, "Diluted EPS", "Basic EPS"))
        if e is not None:
            eps_ttm = e * fx_at(fin_ccy, ccy)
    except Exception:
        pass
    if eps_ttm is None and annual:
        eps_ttm = annual[-1]["eps"]

    # Analystenschätzung für das laufende Geschäftsjahr (für die Fair-Value-Berechnung)
    eps_fwd, eps_next, growth_next = None, None, None
    try:
        ee = t.earnings_estimate
        if ee is not None and not ee.empty:
            est_ccy = ee["currency"].iloc[0] if "currency" in ee.columns else fin_ccy
            est_ccy, est_unit = MINOR_CCY.get(est_ccy, (est_ccy, 1.0))
            f = fx_at(est_ccy, ccy) * est_unit
            if "0y" in ee.index:
                eps_fwd = num(ee.loc["0y", "avg"])
                eps_fwd = eps_fwd * f if eps_fwd is not None else None
            if "+1y" in ee.index:
                eps_next = num(ee.loc["+1y", "avg"])
                eps_next = eps_next * f if eps_next is not None else None
                growth_next = num(ee.loc["+1y", "growth"])
    except Exception:
        pass
    # Unplausible Schätzungen verwerfen (z. B. Währungs-/Datenfehler)
    # Referenz: typischer Gewinn je Aktie (Median aus TTM und den letzten 3 Jahren).
    # Damit fallen Sondereffekte (Abschreibungen, Einmalgewinne) nicht so stark ins Gewicht.
    ref_vals = [e for e in [eps_ttm] + [a["eps"] for a in annual[-3:]] if e is not None and e > 0]
    eps_ref = float(np.median(ref_vals)) if ref_vals else None
    eps_max = max([a["eps"] for a in annual if a["eps"] and a["eps"] > 0] or [0])

    def plausible(e):
        # Schätzungen sind meist "bereinigt" (ohne Sondereffekte). Wir akzeptieren sie, wenn sie
        # in der Nähe des typischen Gewinns liegen oder einen früher schon erreichten Gewinn nicht
        # deutlich übersteigen (z. B. nach einem Abschreibungsjahr).
        if e is None or e <= 0:
            return False
        if eps_ref and 1 / 3 < e / eps_ref < 3:
            return True
        return eps_max > 0 and e <= eps_max * 1.5
    if not plausible(eps_fwd):
        eps_fwd = None
    if not plausible(eps_next):
        eps_next = None
    # Erwarteter Gewinn der nächsten 12 Monate: Mischung aus laufendem und nächstem
    # Geschäftsjahr, je nachdem wie viel vom laufenden Geschäftsjahr noch übrig ist.
    eps_ntm = eps_fwd
    fye = info.get("nextFiscalYearEnd") or info.get("lastFiscalYearEnd")
    if eps_fwd and eps_next and fye:
        days_left = (dt.datetime.fromtimestamp(fye) - dt.datetime.now()).days % 365
        w = max(0.0, min(1.0, days_left / 365))
        eps_ntm = w * eps_fwd + (1 - w) * eps_next
    elif eps_next and not eps_fwd:
        eps_ntm = eps_next
    if growth_next is not None and abs(growth_next) > 1:
        growth_next = None

    # ---- Dividenden ----------------------------------------------------
    divs = t.dividends
    div_years, div_ttm = [], 0.0
    if divs is not None and len(divs):
        divs = divs * unit
        divs.index = divs.index.tz_localize(None)
        now = pd.Timestamp.now()
        div_ttm = float(divs[divs.index > now - pd.Timedelta(days=365)].sum())
        by_year = divs.groupby(divs.index.year).sum()
        counts = divs.groupby(divs.index.year).count()
        cur_year = now.year
        # Übliche Zahlungsfrequenz (z. B. 4x pro Jahr). Fällt eine Zahlung durch den
        # Stichtag ins Folge-/Vorjahr, wird die Jahressumme auf die übliche Frequenz
        # normalisiert, damit keine Schein-Kürzungen/-Erhöhungen entstehen.
        recent = counts[(counts.index < cur_year) & (counts.index >= cur_year - 10)]
        freq = int(recent.median()) if len(recent) else None
        if freq and freq >= 2:
            for y in by_year.index:
                c = counts.get(y, 0)
                if c and c != freq and y < cur_year and abs(c - freq) == 1:
                    by_year[y] = by_year[y] / c * freq
        for y, v in by_year.items():
            if y >= cur_year or y < cur_year - 35:
                continue
            avg_p = closes[closes.index.year == y].mean() if len(closes) else None
            div_years.append({
                "year": int(y), "dps": float(v),
                "yield": (float(v) / avg_p) if (avg_p and not math.isnan(avg_p)) else None,
            })
        # Lücken (Jahre ohne Zahlung) mit 0 auffüllen
        if div_years:
            have = {d["year"] for d in div_years}
            for y in range(div_years[0]["year"], cur_year):
                if y not in have:
                    div_years.append({"year": y, "dps": 0.0, "yield": 0.0})
            div_years.sort(key=lambda d: d["year"])

    pays_dividend = div_ttm > 0
    div_yield = div_ttm / price if pays_dividend else 0.0

    # Jahre ohne Kürzung / mit Steigerung (Toleranz 3 % wegen Währungs-/Stichtagseffekten)
    years_growth = years_no_cut = 0
    if div_years:
        seq = [d["dps"] for d in div_years]
        for i in range(len(seq) - 1, 0, -1):
            if seq[i] > seq[i - 1] * 1.005 and seq[i - 1] > 0:
                years_growth += 1
            else:
                break
        for i in range(len(seq) - 1, 0, -1):
            if seq[i] >= seq[i - 1] * 0.97 and seq[i - 1] > 0:
                years_no_cut += 1
            else:
                break

    def div_cagr(n):
        if len(div_years) < n + 1:
            return None
        return cagr(div_years[-1 - n]["dps"], div_years[-1]["dps"], n)

    div_cagr5, div_cagr10 = div_cagr(5), div_cagr(10)

    payout_eps = (div_ttm / eps_ttm) if (pays_dividend and eps_ttm and eps_ttm > 0) else None
    lfcf, ldiv = last(fcf), last(div_paid)
    payout_fcf = (abs(ldiv) / lfcf) if (ldiv and lfcf and lfcf > 0) else None

    hist_yields = [d["yield"] for d in div_years[-5:] if d["yield"]]
    avg_yield5 = float(np.mean(hist_yields)) if len(hist_yields) >= 3 else None

    # ---- Qualitätskennzahlen ------------------------------------------
    sector_en = info.get("sector") or ""
    is_fin = sector_en in ("Financial Services",)
    is_reit = sector_en == "Real Estate"

    ni_l, rev_l = last(net_income), last(revenue)
    eq_s = equity.dropna()
    avg_eq = float(eq_s.iloc[-2:].mean()) if len(eq_s) else None
    roe = (ni_l / avg_eq) if (ni_l is not None and avg_eq and avg_eq > 0) else None
    op_margin = (last(op_income) / rev_l) if (last(op_income) is not None and rev_l) else None
    fcf_margin = (lfcf / rev_l) if (lfcf is not None and rev_l) else None
    ebitda_l = last(ebitda)
    net_debt = ((last(debt) or 0) - (last(cash) or 0)) if not debt.dropna().empty else None
    nd_ebitda = (net_debt / ebitda_l) if (net_debt is not None and ebitda_l and ebitda_l > 0) else None
    equity_ratio = (last(equity) / last(assets)) if (last(equity) and last(assets)) else None

    eps_list = [a["eps"] for a in annual10 if a["eps"] is not None]
    rev_list = [a["revenue"] for a in annual10 if a["revenue"] is not None]
    eps_stab = r_squared(eps_list)
    neg_eps_years = sum(1 for e in eps_list if e <= 0)
    rev_up_share = (sum(1 for i in range(1, len(rev_list)) if rev_list[i] > rev_list[i - 1]) /
                    (len(rev_list) - 1)) if len(rev_list) > 1 else None
    span = (annual10[-1]["year"] - annual10[0]["year"]) if len(annual10) > 1 else 0
    rev_cagr = cagr(rev_list[0], rev_list[-1], span) if len(rev_list) > 1 else None
    eps_cagr = cagr(eps_list[0], eps_list[-1], span) if len(eps_list) > 1 else None

    # ---- Fairer Wert ---------------------------------------------------
    pes = [a["pe"] for a in annual10 if a["pe"] and 0 < a["pe"] < 80]
    pcfs = [a["pcf"] for a in annual10 if a["pcf"] and 0 < a["pcf"] < 80]
    avg_pe_raw = float(np.median(pes)) if len(pes) >= 2 else None
    avg_pcf_raw = float(np.median(pcfs)) if len(pcfs) >= 2 else None
    # Obergrenze: Auch für Top-Firmen wird kein KGV über 30 / KCV über 25 als "fair" angesetzt,
    # sonst werden Bewertungs-Übertreibungen der Vergangenheit fortgeschrieben.
    PE_CAP, PCF_CAP = 30.0, 25.0
    avg_pe = min(avg_pe_raw, PE_CAP) if avg_pe_raw else None
    avg_pcf = min(avg_pcf_raw, PCF_CAP) if avg_pcf_raw else None
    basis_kind = "forecast"
    eps_basis = eps_ntm
    if not eps_basis:
        eps_basis, basis_kind = eps_ttm, "ttm"
        if eps_ref and (eps_ttm is None or not (0.6 * eps_ref <= eps_ttm <= 1.67 * eps_ref)):
            eps_basis, basis_kind = eps_ref, "normalized"
    ocf_ps_l = annual[-1]["ocfPerShare"] if annual else None

    methods = []
    if avg_pe and eps_basis and eps_basis > 0:
        methods.append({
            "key": "pe", "name": "KGV-Methode", "value": avg_pe * eps_basis, "weight": 0.45,
            "multiple": avg_pe, "multipleRaw": avg_pe_raw, "capped": avg_pe_raw > PE_CAP,
            "basis": eps_basis, "basisKind": basis_kind,
        })
    if avg_pcf and ocf_ps_l and ocf_ps_l > 0 and not is_fin:
        methods.append({
            "key": "pcf", "name": "KCV-Methode", "value": avg_pcf * ocf_ps_l, "weight": 0.30,
            "multiple": avg_pcf, "multipleRaw": avg_pcf_raw, "capped": avg_pcf_raw > PCF_CAP, "basis": ocf_ps_l,
        })
    # Nicht nach einer frischen Kürzung oder wenn die Dividende nicht verdient wird
    div_sustainable = years_no_cut >= 2 and (is_reit or payout_eps is None or payout_eps <= 1.0)
    if avg_yield5 and pays_dividend and avg_yield5 > 0.005 and div_sustainable:
        methods.append({
            "key": "yield", "name": "Dividendenrendite-Methode", "value": div_ttm / avg_yield5,
            "weight": 0.25, "avgYield": avg_yield5, "basis": div_ttm,
        })
    fair_value = None
    if methods:
        w = sum(m["weight"] for m in methods)
        fair_value = sum(m["value"] * m["weight"] for m in methods) / w
    upside = (fair_value / price - 1) if fair_value else None

    # Linie für den Chart: Gewinn je Aktie des Jahres x Ø-KGV
    fv_line = []
    if avg_pe:
        fv_line = [[a["date"][:7], a["eps"] * avg_pe] for a in annual if a["eps"] and a["eps"] > 0]
        if eps_fwd:
            fv_line.append([dt.date.today().strftime("%Y-%m"), eps_fwd * avg_pe])
        if eps_next and eps_next > 0:
            fv_line.append([(dt.date.today() + dt.timedelta(days=365)).strftime("%Y-%m"), eps_next * avg_pe])

    # ---- Scores --------------------------------------------------------
    q_parts = [
        (lin(roe, 0.05, 0.20), 20),
        (None if is_fin else lin(op_margin, 0.05, 0.25), 15),
        (None if is_fin else lin(fcf_margin, 0.0, 0.18), 15),
        (None if (is_fin or nd_ebitda is None) else (1.0 if nd_ebitda <= 1 else 1 - lin(nd_ebitda, 1, 4)), 15),
        (None if eps_stab is None else eps_stab * (0.5 ** neg_eps_years), 20),
        (rev_up_share, 15),
    ]
    quality = weighted(q_parts)

    if pays_dividend:
        y_s = lin(div_yield, 0.01, 0.035)
        if div_yield > 0.07:  # sehr hohe Renditen sind oft ein Warnsignal
            y_s *= max(0.3, 1 - (div_yield - 0.07) * 10)
        po = payout_fcf if is_reit else payout_eps
        po_s = None
        if po is not None:
            po_s = 1.0 if po <= 0.65 else max(0.0, 1 - (po - 0.65) / 0.35)
        d_parts = [
            (y_s, 20), (po_s, 20),
            (None if is_fin or payout_fcf is None else 1 - lin(payout_fcf, 0.7, 1.1), 15),
            (lin(years_growth, 0, 25), 20),
            (lin(years_no_cut, 0, 15), 10),
            (lin(div_cagr5, 0, 0.10), 15),
        ]
        dividend = weighted(d_parts)
    else:
        dividend = 0

    g_parts = [
        (lin(rev_cagr, 0, 0.10), 30),
        (lin(eps_cagr, 0, 0.12), 30),
        (lin(growth_next, 0, 0.12), 25),
        (lin(div_cagr5, 0, 0.10) if pays_dividend else None, 15),
    ]
    growth = weighted(g_parts)
    valuation = round(100 * lin(upside, -0.25, 0.30)) if upside is not None else None

    total_parts = [(quality / 100 if quality is not None else None, 35),
                   (dividend / 100 if dividend is not None else None, 20),
                   (growth / 100 if growth is not None else None, 20),
                   (valuation / 100 if valuation is not None else None, 25)]
    total = weighted(total_parts)

    country = info.get("country") or ""
    return {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName") or symbol,
        "country": COUNTRY_DE.get(country, country),
        "region": REGION.get(country, "Sonstige"),
        "sector": SECTOR_DE.get(sector_en, sector_en or "Sonstige"),
        "industry": info.get("industry"),
        "description": (info.get("longBusinessSummary") or "")[:600],
        "currency": ccy, "finCurrency": fin_ccy,
        "price": price,
        "marketCap": num(info.get("marketCap")),
        "marketCapEUR": (num(info.get("marketCap")) or 0) * unit * fx_at(ccy, "EUR") if info.get("marketCap") else None,
        "isFinancial": is_fin, "isReit": is_reit,
        "scores": {"total": total, "quality": quality, "dividend": dividend,
                   "growth": growth, "valuation": valuation},
        "valuation": {
            "fairValue": fair_value, "upside": upside, "methods": methods,
            "pe": (price / eps_ttm) if (eps_ttm and eps_ttm > 0) else None,
            "peFwd": (price / eps_ntm) if (eps_ntm and eps_ntm > 0) else None,
            "avgPe": avg_pe, "avgPeRaw": avg_pe_raw, "peYears": len(pes), "pcfYears": len(pcfs), "epsRef": eps_ref, "pcf": (price / ocf_ps_l) if (ocf_ps_l and ocf_ps_l > 0) else None,
            "avgPcf": avg_pcf, "avgPcfRaw": avg_pcf_raw, "epsTtm": eps_ttm, "epsFwd": eps_fwd, "epsNext": eps_next, "epsNtm": eps_ntm,
            "analystTarget": (num(info.get("targetMeanPrice")) or 0) * unit or None,
            "analystCount": info.get("numberOfAnalystOpinions"),
            "fvLine": fv_line,
        },
        "quality": {
            "roe": roe, "opMargin": op_margin, "fcfMargin": fcf_margin,
            "netDebtEbitda": nd_ebitda, "equityRatio": equity_ratio,
            "epsStability": eps_stab, "negEpsYears": neg_eps_years,
            "revUpShare": rev_up_share,
        },
        "growth": {"revCagr": rev_cagr, "epsCagr": eps_cagr, "epsGrowthNext": growth_next,
                   "years": span},
        "dividend": {
            "pays": pays_dividend, "dps": div_ttm, "yield": div_yield,
            "avgYield5": avg_yield5, "payoutEps": payout_eps, "payoutFcf": payout_fcf,
            "yearsGrowth": years_growth, "yearsNoCut": years_no_cut,
            "cagr5": div_cagr5, "cagr10": div_cagr10,
            "history": div_years[-20:],
        },
        "annual": annual, "annualRaw": annual_raw, "secUsed": sec_used,
        "priceHistory": [p for p in price_hist if not annual or p[0] >= str(annual[0]["year"] - 1)],
    }


REGION_BY_DE = {**REGION, **{COUNTRY_DE.get(k, k): v for k, v in REGION.items()}}
CYCLICAL = {"Grundstoffe", "Energie"}


def finalize(r: dict) -> dict:
    """Leitet Bewertung und Noten aus den gespeicherten Bausteinen ab (idempotent,
    gilt auch für ältere, zwischengespeicherte Datensätze)."""
    r["country"] = COUNTRY_DE.get(r["country"], r["country"])
    r["region"] = REGION_BY_DE.get(r["country"], "Sonstige")
    v, sc, price = r["valuation"], r["scores"], r["price"]
    if "methodsAll" not in v:
        v["methodsAll"] = v["methods"]
    # Plausibilität: Methoden, die mehr als das 4-fache oder weniger als ein Viertel des
    # Kurses ergeben, beruhen fast immer auf Datenfehlern (Währung, Aktiensplit, Sondereffekt).
    ok = [m for m in v["methodsAll"] if price and 0.25 <= m["value"] / price <= 4]
    v["methods"] = ok
    v["dropped"] = len(v["methodsAll"]) - len(ok)
    fv = sum(m["value"] * m["weight"] for m in ok) / sum(m["weight"] for m in ok) if ok else None
    v["fairValue"] = fv
    v["upside"] = (fv / price - 1) if fv else None
    sc["valuation"] = round(100 * lin(v["upside"], -0.25, 0.30)) if v["upside"] is not None else None
    # Zyklische Branchen: Gewinne schwanken mit Rohstoffpreisen, wenige Jahre Historie
    # überzeichnen die Stabilität. Deshalb 15 % Abschlag auf die Qualitätsnote.
    if "qualityRaw" not in sc:
        sc["qualityRaw"] = sc["quality"]
    q = sc["qualityRaw"]
    r["cyclical"] = r["sector"] in CYCLICAL
    sc["quality"] = round(q * 0.85) if (q is not None and r["cyclical"]) else q
    parts = [(sc["quality"] / 100 if sc["quality"] is not None else None, 35),
             (sc["dividend"] / 100 if sc["dividend"] is not None else None, 20),
             (sc["growth"] / 100 if sc["growth"] is not None else None, 20),
             (sc["valuation"] / 100 if sc["valuation"] is not None else None, 25)]
    sc["total"] = weighted(parts)
    return r


N_SHARDS = 40
DATA_DIR = ROOT / "data"


def summary(r: dict, shard: int) -> dict:
    """Kompakte Zeile für den Screener (die Details liegen in den Shard-Dateien)."""
    v, d, q, g = r["valuation"], r["dividend"], r["quality"], r["growth"]
    return {
        "symbol": r["symbol"], "name": r["name"], "country": r["country"], "region": r["region"],
        "sector": r["sector"], "industry": r["industry"], "currency": r["currency"], "price": r["price"],
        "marketCapEUR": r["marketCapEUR"], "isFinancial": r["isFinancial"], "isReit": r["isReit"], "cyclical": r.get("cyclical"),
        "scores": r["scores"],
        "valuation": {k: v.get(k) for k in ("fairValue", "upside", "pe", "avgPe", "peFwd")},
        "dividend": {k: d.get(k) for k in ("pays", "yield", "payoutEps", "yearsGrowth", "yearsNoCut", "cagr5")},
        "quality": {"roe": q.get("roe")}, "growth": {"epsCagr": g.get("epsCagr"), "revCagr": g.get("revCagr")},
        "shard": shard,
    }


HISTORY = DATA_DIR / "history" / "annual.json"


def load_previous() -> dict:
    """Bisherige Detaildaten (damit bei einem Abruf-Fehler der alte Stand erhalten bleibt)
    und das dauerhafte Archiv der Jahresabschlüsse (wächst jedes Jahr)."""
    prev = {}
    for f in (DATA_DIR / "details").glob("*.json"):
        try:
            for r in json.loads(f.read_text()):
                prev[r["symbol"]] = r
        except Exception:
            pass
    if HISTORY.exists():
        try:
            for sym, rows in json.loads(HISTORY.read_text()).items():
                p = prev.setdefault(sym, {})
                have = {x["date"] for x in p.get("annualRaw", [])}
                p["annualRaw"] = sorted(p.get("annualRaw", []) + [x for x in rows if x["date"] not in have], key=lambda x: x["date"])
        except Exception as e:
            print("Archiv nicht lesbar:", e)
    return prev


def save_history(records):
    """Archiv: eine Zeile je Aktie, damit Änderungen im Git-Verlauf klein und lesbar bleiben."""
    HISTORY.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for r in sorted(records, key=lambda r: r["symbol"]):
        rows = r.get("annualRaw") or []
        if rows:
            lines.append(json.dumps(r["symbol"]) + ":" + json.dumps(clean(rows), ensure_ascii=False, separators=(",", ":")))
    HISTORY.write_text("{\n" + ",\n".join(lines) + "\n}\n")


def read_symbols():
    syms, names = [], set()
    sys.path.insert(0, str(ROOT / "pipeline"))
    from universe import norm_name
    uni = ROOT / "pipeline" / "universe.csv"
    if uni.exists():
        with open(uni) as fh:
            for r in csv.DictReader(fh):
                syms.append(r["ticker"].strip())
                names.add(norm_name(r["name"]))
    # Wunschliste: zusätzliche Aktien (auch kleinere), sofern nicht schon enthalten
    with open(ROOT / "pipeline" / "tickers.csv") as fh:
        for r in csv.DictReader(fh):
            t = r["ticker"].strip()
            if t and t not in syms:
                syms.append(t)
    return syms, norm_name


def main():
    only = [a for a in sys.argv[1:] if not a.startswith("-")]
    all_syms, norm_name = read_symbols()
    symbols = [] if "--rebuild" in sys.argv else (only or all_syms)  # --rebuild: nur neu berechnen
    prev = load_previous()
    workers = 3

    results, errors = {}, []

    def work(sym):
        err = None
        for attempt in range(4):
            try:
                return analyse(sym, prev.get(sym))
            except Exception as e:
                err = e
                if "keine Kursdaten" in str(e) or "kein Kurs" in str(e):
                    break
                time.sleep(5 * (attempt + 1))
        raise err

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(work, s): s for s in symbols}
        for i, f in enumerate(as_completed(futs), 1):
            s = futs[f]
            try:
                results[s] = f.result()
                print(f"[{i}/{len(symbols)}] ok   {s}", flush=True)
            except Exception as e:
                errors.append({"symbol": s, "error": str(e)})
                print(f"[{i}/{len(symbols)}] FEHLER {s}: {e}", flush=True)
                if "-v" in sys.argv:
                    traceback.print_exc()

    # Bei Teilläufen oder Fehlern: alten Stand übernehmen
    keep = set(all_syms) | set(results)
    for sym, r in prev.items():
        if sym not in results and sym in keep and "scores" in r:
            results[sym] = r

    # Doppelte Unternehmen (z. B. zwei Aktiengattungen) zusammenfassen: das erste gewinnt
    order = {s: i for i, s in enumerate(all_syms)}
    final, seen = [], set()
    for r in sorted(results.values(), key=lambda r: order.get(r["symbol"], 10**6)):
        key = norm_name(r["name"])
        if key in seen:
            continue
        seen.add(key)
        final.append(r)
    final = [finalize(r) for r in final]
    save_history(final)
    final.sort(key=lambda r: -(r["scores"]["total"] or 0))

    (DATA_DIR / "details").mkdir(parents=True, exist_ok=True)
    for f in (DATA_DIR / "details").glob("*.json"):
        f.unlink()
    shards = [[] for _ in range(N_SHARDS)]
    rows = []
    for r in final:
        sh = zlib.crc32(r["symbol"].encode()) % N_SHARDS
        shards[sh].append(r)
        rows.append(summary(r, sh))
    for i, items in enumerate(shards):
        (DATA_DIR / "details" / f"{i}.json").write_text(json.dumps(clean(items), ensure_ascii=False, separators=(",", ":")))
    payload = {
        "generated": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": "Yahoo Finance (über yfinance)",
        "count": len(rows), "errors": errors, "stocks": rows,
    }
    (DATA_DIR / "index.json").write_text(json.dumps(clean(payload), ensure_ascii=False, separators=(",", ":")))
    old = DATA_DIR / "stocks.json"
    if old.exists():
        old.unlink()
    print(f"\n{len(rows)} Aktien gespeichert in {DATA_DIR}, {len(errors)} Fehler.")


if __name__ == "__main__":
    main()
