# 📦 Shipping Invoice Analyzer

Streamlit-app som analyserar fraktfakturor (PDF) och visar hur totalkostnaden fördelas per land.

## Stödda format
- **UPS** — svenska fakturor med specifikationssektioner (fullständigt stöd)
- **DHL** — under utveckling

## Kör lokalt

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy till Streamlit Cloud

1. Pusha denna mapp till ett GitHub-repo
2. Gå till [share.streamlit.io](https://share.streamlit.io)
3. Peka på repot och `app.py`
4. Klart — appen är live och delbar

## Hur det fungerar

Appen extraherar text ur PDF:en med `pdfplumber` och letar efter mönstret:

```
Mottagare:  NAMN  STAD  POSTNR  LAND
...
Total kostnad för sändning 1ZXXXXX  SEK  Brutto  Rabatt  Netto
```

Varje sändning kopplas till mottagarlandet och nettobeloppet aggregeras per land.

**OBS:** Adressändringar, korrigeringar och justeringar som inte är kopplade till en specifik sändning ingår inte i landsfördelningen. Typiskt täcker parsern ~95% av fakturans totalbelopp.
