# Wertkompass

Persönlicher Aktien-Screener für **Qualitätsaktien zum fairen Preis**, mit Schwerpunkt auf Dividenden. Alle Erklärungen sind auf Deutsch, die Fachbegriffe kannst du antippen und im Lexikon nachlesen.

## Was die App macht

- Rund 1.500 Unternehmen ab 10 Mrd. € Börsenwert aus 23 Ländern (automatisch über den Yahoo-Screener) plus eine Wunschliste kleinerer Werte
- Filter nach Region, Branche, Mindest-Qualität, Mindest-Dividendenrendite, „nur unterbewertete“
- Vier Teilnoten (0–100): **Qualität**, **Dividende**, **Wachstum**, **Bewertung**
- Fairer Wert aus drei Methoden: historisches KGV, historisches KCV und historische Dividendenrendite
- Steckbrief pro Aktie mit Erklärungen in einfacher Sprache, Diagrammen (Kurs gegen faire Linie, Dividendenhistorie, Umsatz und Gewinn) und Stärken/Schwächen

## Aufbau

| Datei | Zweck |
| --- | --- |
| `index.html` | Die komplette Web-App (eine Datei) |
| `data/index.json`, `data/details/` | Berechnete Kennzahlen (Übersicht + Details), werden automatisch erzeugt |
| `pipeline/universe.py` | Stellt das Aktien-Universum über den Yahoo-Screener zusammen (Mindest-Börsenwert einstellbar) |
| `pipeline/universe.csv` | Ergebnis davon |
| `pipeline/tickers.csv` | Wunschliste: zusätzliche Aktien, die immer dabei sind |
| `pipeline/fetch.py` | Lädt die Daten von Yahoo Finance und berechnet Scores und fairen Wert |
| `pipeline/build_site.py` | Baut die Seite für GitHub Pages |
| `.github/workflows/update.yml` | Aktualisiert die Daten jeden Werktag automatisch und veröffentlicht die Seite |

## Aktie hinzufügen

Kürzel wie bei Yahoo Finance in `pipeline/tickers.csv` eintragen, z. B. `ALV.DE` (Xetra), `NESN.SW` (Schweiz), `ULVR.L` (London), `7203.T` (Tokio), `MSFT` (USA). Beim nächsten automatischen Lauf ist sie dabei.

## Lokal ausführen

```bash
pip install -r pipeline/requirements.txt
python pipeline/fetch.py
python pipeline/build_site.py
python -m http.server -d _site 8000
```

Dann im Browser `http://localhost:8000` öffnen.

## Hinweis

Lern- und Analysewerkzeug, keine Anlageberatung. Die Daten stammen aus Yahoo Finance und können Fehler enthalten.
