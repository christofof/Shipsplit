"""
Shipsplit — Shipping Invoice Analyzer
Parses shipping invoices (UPS, Bring) and breaks down costs by country.
"""

import streamlit as st
import pdfplumber
import pandas as pd
import plotly.express as px
import re
from collections import defaultdict

# ─── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Shipsplit",
    page_icon="📦",
    layout="wide",
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');
    html, body, [class*="css"] { font-family: 'DM Sans', sans-serif; }
    .block-container { padding-top: 2rem; max-width: 1100px; }
    h1 { font-weight: 700; letter-spacing: -0.02em; }
    h2, h3 { font-weight: 600; }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border: 1px solid #dee2e6; border-radius: 12px; padding: 1rem 1.25rem;
    }
    div[data-testid="stMetric"] label {
        font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.05em; color: #6c757d;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] { font-weight: 700; font-size: 1.6rem; }
    [data-testid="stFileUploader"] { border: 2px dashed #adb5bd; border-radius: 12px; padding: 1rem; }
    [data-testid="stFileUploader"]:hover { border-color: #495057; }
</style>
""", unsafe_allow_html=True)


# ─── Country normalization ──────────────────────────────────────────────────

COUNTRY_NAME_MAP = {
    # UPS Swedish names
    "IRLÄNDSKA": "Irland", "FÖRENADE": "Förenade Arabemiraten",
    "SLOVAKISKA": "Slovakien", "TJECKISKA": "Tjeckien",
    "STORBRITANNIEN": "Storbritannien", "NEDERLÄNDERNA": "Nederländerna",
    "TYSKLAND": "Tyskland", "SYDKOREA": "Sydkorea", "SPANIEN": "Spanien",
    "ITALIEN": "Italien", "FRANKRIKE": "Frankrike", "BELGIEN": "Belgien",
    "GREKLAND": "Grekland", "SLOVENIEN": "Slovenien", "LITAUEN": "Litauen",
    "PORTUGAL": "Portugal", "LUXEMBURG": "Luxemburg", "KROATIEN": "Kroatien",
    "ÖSTERRIKE": "Österrike", "UNGERN": "Ungern", "RUMÄNIEN": "Rumänien",
    "SCHWEIZ": "Schweiz", "ISLAND": "Island", "ISRAEL": "Israel",
    "ESTLAND": "Estland", "POLEN": "Polen", "KANADA": "Kanada",
    "KAZAKHSTAN": "Kazakstan", "TAIWAN": "Taiwan", "MALTA": "Malta",
    "USA": "USA", "JAPAN": "Japan", "SVERIGE": "Sverige",
    "LETTLAND": "Lettland", "BULGARIEN": "Bulgarien", "CYPERN": "Cypern",
    "FINLAND": "Finland", "NORGE": "Norge", "DANMARK": "Danmark",
    "KINA": "Kina", "AUSTRALIEN": "Australien", "SINGAPORE": "Singapore",
    "TURKIET": "Turkiet", "INDIEN": "Indien", "MEXIKO": "Mexiko",
    "BRASILIEN": "Brasilien", "HONGKONG": "Hongkong", "THAILAND": "Thailand",
    "VIETNAM": "Vietnam", "FILIPPINERNA": "Filippinerna",
    "MALAYSIA": "Malaysia", "INDONESIEN": "Indonesien",
    # Bring ISO codes
    "SE": "Sverige", "NO": "Norge", "DK": "Danmark", "FI": "Finland",
    "DE": "Tyskland", "NL": "Nederländerna", "GB": "Storbritannien",
    "FR": "Frankrike", "ES": "Spanien", "IT": "Italien", "PL": "Polen",
    "AT": "Österrike", "BE": "Belgien", "IE": "Irland", "PT": "Portugal",
    "GR": "Grekland", "CZ": "Tjeckien", "SK": "Slovakien", "HU": "Ungern",
    "RO": "Rumänien", "BG": "Bulgarien", "HR": "Kroatien", "SI": "Slovenien",
    "LT": "Litauen", "LV": "Lettland", "EE": "Estland", "LU": "Luxemburg",
    "MT": "Malta", "CY": "Cypern", "IS": "Island", "CH": "Schweiz",
    "US": "USA", "CA": "Kanada", "JP": "Japan", "KR": "Sydkorea",
    "CN": "Kina", "AU": "Australien", "SG": "Singapore", "TW": "Taiwan",
    "HK": "Hongkong", "TH": "Thailand", "VN": "Vietnam",
    "AE": "Förenade Arabemiraten", "IL": "Israel", "TR": "Turkiet",
    "IN": "Indien", "MX": "Mexiko", "BR": "Brasilien",
}


def normalize_country(raw: str) -> str:
    raw = raw.strip().upper()
    return COUNTRY_NAME_MAP.get(raw, raw.title())


def parse_swedish_number(s: str) -> float:
    return float(s.strip().replace(" ", "").replace(".", "").replace(",", "."))


# ─── Carrier detection ──────────────────────────────────────────────────────

def detect_carrier(text: str) -> str:
    lower = text[:5000].lower()
    if "ups" in lower and ("sändning" in lower or "express saver" in lower):
        return "UPS"
    if "bring" in lower and ("pickup parcel" in lower or "bring e-commerce" in lower):
        return "Bring"
    if "dhl" in lower:
        if "dhl freight" in lower or "fraktsedelsnr" in lower or "servpoint" in lower:
            return "DHL Freight"
        if "waybill" in lower or "shipment" in lower or "express" in lower:
            return "DHL Express"
        return "DHL"
    return "Unknown"


# ─── UPS Parser ─────────────────────────────────────────────────────────────

def parse_ups_invoice(pdf_file) -> pd.DataFrame:
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")
    records = []

    # --- Format 1: Outbound invoices (Specifikation sections) ---
    # Pattern: Mottagare: ... COUNTRY → Total kostnad för sändning TRACKING SEK ...
    mottagare_re = re.compile(r"Mottagare:\s+.+\s(\S+)\s*$")
    total_re = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)"
    )
    total_re_2 = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s*$"
    )

    current_country = None
    for line in lines:
        mm = mottagare_re.search(line)
        if mm:
            current_country = normalize_country(mm.group(1))
            continue

        if "Total kostnad för sändning" in line and current_country:
            tm = total_re.search(line)
            if tm:
                records.append({
                    "Land": current_country,
                    "Belopp (SEK)": parse_swedish_number(tm.group(4)),
                    "Kolli": 1,
                    "Typ": "Utgående",
                    "Detalj": tm.group(1),
                })
                current_country = None
                continue

            tm2 = total_re_2.search(line)
            if tm2:
                records.append({
                    "Land": current_country,
                    "Belopp (SEK)": parse_swedish_number(tm2.group(3)),
                    "Kolli": 1,
                    "Typ": "Utgående",
                    "Detalj": tm2.group(1),
                })
                current_country = None

    # --- Format 2: Returns invoices (UPS Returns / import / undeliverable) ---
    # Pattern: Skickat från: NAME CITY POSTAL COUNTRY → Totalkostnad SEK ...
    skickat_re = re.compile(r"Skickat från:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    avsandare_re = re.compile(r"Avsändare:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    totalkostnad_re = re.compile(
        r"Totalkostnad\s+SEK\s+[\d.,]+\s+(?:[\d.,]+\s+)?[\d.,]+\s+([\d.,]+)\s*$"
    )

    current_country = None
    current_section = None

    for line in lines:
        s = line.strip()

        # Track section type
        if "UPS Returns" in s:
            current_section = "Retur"
        elif "importsändningar" in s.lower():
            current_section = "Retur"
        elif "icke levererbara returer" in s.lower():
            current_section = "Retur"
        elif "upphämtningsbegäran" in s.lower():
            current_section = "Övrigt"

        # Origin country from "Skickat från:" or "Avsändare:"
        sm = skickat_re.search(line)
        if sm:
            current_country = normalize_country(sm.group(1))
            continue
        am = avsandare_re.search(line)
        if am:
            current_country = normalize_country(am.group(1))
            continue

        # Totalkostnad line
        if "Totalkostnad" in line and "SEK" in line and current_country:
            tm = totalkostnad_re.search(line)
            if tm:
                records.append({
                    "Land": current_country,
                    "Belopp (SEK)": parse_swedish_number(tm.group(1)),
                    "Kolli": 1,
                    "Typ": current_section or "Retur",
                    "Detalj": current_section or "Retur",
                })
                current_country = None

    # Extract invoice total (ex VAT) from page 1 summary
    invoice_total = None
    momspliktigt = 0.0
    icke_moms = 0.0
    for line in lines:
        m = re.search(r"Totalt momspliktigt\s+([\d.,]+)", line)
        if m:
            momspliktigt = parse_swedish_number(m.group(1))
        m2 = re.search(r"Icke momspliktigt\s+([\d.,]+)", line)
        if m2:
            icke_moms = parse_swedish_number(m2.group(1))
    if momspliktigt > 0 or icke_moms > 0:
        invoice_total = momspliktigt + icke_moms

    return pd.DataFrame(records), invoice_total


# ─── Bring Parser ───────────────────────────────────────────────────────────

def parse_bring_invoice(pdf_file) -> pd.DataFrame:
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")

    line_re = re.compile(
        r"^(\d+)\s+"               # row number
        r"\d+\s+"                   # article nr
        r"\d+\s+"                   # service code
        r"(.+?)\s+"                 # service name
        r"([A-Z]{2})\s+"           # from country
        r"([A-Z]{2})\s+"           # to country
        r"(\d+)\s+"                 # quantity
        r"St\s+"                    # unit
        r"([\d,]+)\s+"             # price per unit
        r"(?:[\d.]+\s+)?"          # optional discount %
        r"(?:Export|Local VAT)\s+"  # VAT type
        r"([\d\s,]+)$"             # amount
    )

    records = []
    for line in lines:
        line = line.strip()
        m = line_re.match(line)
        if m:
            service = m.group(2).strip()
            from_c = m.group(3)
            to_c = m.group(4)
            qty = int(m.group(5))
            amount = float(m.group(7).replace(" ", "").replace(",", "."))

            # Customer country: destination for outbound, origin for returns
            is_return = "Return" in service
            if from_c == to_c:
                customer = from_c
            elif is_return:
                customer = from_c if from_c != "SE" else to_c
            else:
                customer = to_c if to_c != "SE" else from_c

            records.append({
                "Land": normalize_country(customer),
                "Belopp (SEK)": amount,
                "Kolli": qty,
                "Typ": "Retur" if is_return else "Utgående",
                "Detalj": service,
            })

    # Extract invoice total from "Summa exkl. moms"
    invoice_total = None
    m = re.search(r"Summa exkl\.?\s*moms\s+([\d\s.,]+)", full_text)
    if m:
        invoice_total = parse_swedish_number(m.group(1))

    return pd.DataFrame(records), invoice_total


# ─── DHL Parser (stub) ─────────────────────────────────────────────────────

def parse_dhl_freight_invoice(pdf_file) -> pd.DataFrame:
    st.info(
        "ℹ️ DHL Freight-faktura identifierad. Denna faktura innehåller enbart "
        "inrikes sändningar (Sverige). Landsfördelning visar 100% Sverige."
    )
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")
    records = []

    # Strategy 1: Parse individual shipment TOTALT from spec pages
    # Each shipment block has A/B/C/D lines. The TOTALT appears as the last
    # comma-decimal number on D-lines or standalone after D-lines.
    current_service = None
    current_city = None

    for line in lines:
        s = line.strip()

        # A-line: service type
        if re.match(r"^A\s+\d{3}\s", s):
            if "SERVPOINT B2C" in s:
                current_service = "Servpoint B2C"
            elif "SERVPOINT C2B" in s:
                current_service = "Servpoint C2B"
            elif "HOME DELIVERY" in s:
                current_service = "Home Delivery"
            else:
                current_service = None
            current_city = None

        # B-line: destination city (third field: B fraktsedelsnr CITY ...)
        elif re.match(r"^B\s+\d", s) and current_service:
            parts = s.split()
            if len(parts) >= 3:
                current_city = parts[2]

        # D-line: ends with "km_distance antal_kolli TOTALT"
        elif re.match(r"^D\s", s) and current_service and "Tilläggs" not in s:
            m = re.search(r"(\d+)\s+(\d+)\s+(\d+,\d{2})\s*$", s)
            if m:
                kolli = int(m.group(2))
                amount = float(m.group(3).replace(",", "."))
                records.append({
                    "Land": "Sverige",
                    "Belopp (SEK)": amount,
                    "Kolli": kolli,
                    "Typ": "Retur" if current_service == "Servpoint C2B" else "Utgående",
                    "Detalj": f"{current_service} → {current_city or '?'}",
                })
                current_service = None
                current_city = None

    # Strategy 2: If spec parsing found nothing, fall back to page 1 summary
    if not records:
        summary_re = re.compile(
            r"(HOME DELIVERY|SERVPOINT B2C|SERVPOINT C2B)\s+(\d+)\s+([\d\s,]+?)(?:\s*\*)?$",
            re.MULTILINE,
        )
        for m in summary_re.finditer(full_text):
            service = m.group(1).title()
            count = int(m.group(2))
            amount = float(m.group(3).strip().replace(" ", "").replace(",", "."))
            records.append({
                "Land": "Sverige",
                "Belopp (SEK)": amount,
                "Kolli": count,
                "Typ": "Retur" if "C2B" in m.group(1).upper() else "Utgående",
                "Detalj": service,
            })

    # Extract invoice total from page 1 summary
    invoice_total = None
    # DHL Freight: "Summa exkl. moms" or "TOTAL" line with SEK amount
    m = re.search(r"Summa exkl\.?\s*moms\s+([\d\s.,]+)", full_text)
    if m:
        invoice_total = parse_swedish_number(m.group(1))
    else:
        # Fallback: "Momspliktigt belopp SEK AMOUNT" on page 1
        m2 = re.search(r"Momspliktigt belopp\s+SEK\s+([\d\s.,]+)", full_text)
        if m2:
            invoice_total = parse_swedish_number(m2.group(1))

    return pd.DataFrame(records), invoice_total


def parse_dhl_express_invoice(pdf_file) -> pd.DataFrame:
    st.warning(
        "⚠️ DHL Express-parsern är inte implementerad än. "
        "Ladda upp en exempelfaktura så bygger vi stöd för den."
    )
    return pd.DataFrame(), None


# ─── Main UI ────────────────────────────────────────────────────────────────

st.title("📦 Shipsplit")
st.markdown(
    "Ladda upp en fraktfaktura (PDF) från **UPS**, **Bring** eller **DHL** "
    "för att se hur kostnaden fördelas per land."
)

uploaded_file = st.file_uploader(
    "Välj PDF-faktura",
    type=["pdf"],
    help="Stödjer UPS och Bring (svenska fakturor). DHL under utveckling.",
)

if uploaded_file is not None:
    with pdfplumber.open(uploaded_file) as pdf:
        sample_text = ""
        for page in pdf.pages[:3]:
            t = page.extract_text()
            if t:
                sample_text += t + "\n"

    carrier = detect_carrier(sample_text)
    uploaded_file.seek(0)

    if carrier == "Unknown":
        st.error(
            "❌ Kunde inte identifiera fraktbolaget. "
            "Stödjer för närvarande UPS, Bring och DHL."
        )
        st.stop()

    st.info(f"🔍 Identifierat: **{carrier}**-faktura. Analyserar...")

    with st.spinner("Extraherar data från PDF..."):
        invoice_total = None
        if carrier == "UPS":
            df, invoice_total = parse_ups_invoice(uploaded_file)
        elif carrier == "Bring":
            df, invoice_total = parse_bring_invoice(uploaded_file)
        elif carrier == "DHL Freight":
            df, invoice_total = parse_dhl_freight_invoice(uploaded_file)
        elif carrier == "DHL Express":
            df, invoice_total = parse_dhl_express_invoice(uploaded_file)
        elif carrier == "DHL":
            df, invoice_total = parse_dhl_express_invoice(uploaded_file)
        else:
            df = pd.DataFrame()

    if df.empty:
        st.warning("Inga sändningar hittades i fakturan.")
        st.stop()

    # ── Summary metrics ──────────────────────────────────────────────────
    st.markdown("---")
    total_amount = df["Belopp (SEK)"].sum()
    total_parcels = int(df["Kolli"].sum())
    n_lines = len(df)
    n_countries = df["Land"].nunique()
    avg_per_parcel = total_amount / total_parcels if total_parcels > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Totalt belopp", f"{total_amount:,.0f} SEK")
    col2.metric("Kolli", f"{total_parcels:,}")
    col3.metric("Länder", f"{n_countries}")
    col4.metric("Snitt / kolli", f"{avg_per_parcel:,.1f} SEK")

    # ── Type split (Utgående / Retur / Övrigt) ───────────────────────────
    if "Typ" in df.columns and df["Typ"].nunique() > 0:
        st.markdown("---")
        st.subheader("Uppdelning per typ")

        type_agg = (
            df.groupby("Typ")
            .agg(
                Kolli=("Kolli", "sum"),
                Rader=("Belopp (SEK)", "count"),
                **{"Belopp (SEK)": ("Belopp (SEK)", "sum")},
            )
            .sort_values("Belopp (SEK)", ascending=False)
            .reset_index()
        )
        type_agg["Andel"] = (type_agg["Belopp (SEK)"] / total_amount * 100).round(1)

        # Show as metric cards
        type_cols = st.columns(len(type_agg))
        for i, row in type_agg.iterrows():
            with type_cols[i]:
                st.metric(
                    row["Typ"],
                    f"{row['Belopp (SEK)']:,.0f} SEK",
                    f"{row['Andel']:.1f}% — {int(row['Kolli']):,} kolli",
                )

    # ── Country breakdown ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Fördelning per land")

    has_types = "Typ" in df.columns and df["Typ"].nunique() > 1

    # Build cross-tab: Land × Typ
    if has_types:
        # Pivot: amount per country per type
        pivot_amount = df.pivot_table(
            index="Land", columns="Typ", values="Belopp (SEK)",
            aggfunc="sum", fill_value=0,
        )
        pivot_kolli = df.pivot_table(
            index="Land", columns="Typ", values="Kolli",
            aggfunc="sum", fill_value=0,
        )

        # Ensure standard column order
        type_order = [t for t in ["Utgående", "Retur", "Övrigt"] if t in pivot_amount.columns]
        pivot_amount = pivot_amount.reindex(columns=type_order, fill_value=0)
        pivot_kolli = pivot_kolli.reindex(columns=type_order, fill_value=0)

        pivot_amount["Totalt"] = pivot_amount.sum(axis=1)
        pivot_kolli["Totalt kolli"] = pivot_kolli.sum(axis=1)
        pivot_amount["Andel"] = (pivot_amount["Totalt"] / total_amount * 100).round(1)

        country_table = pivot_amount.sort_values("Totalt", ascending=False).reset_index()
        kolli_sorted = pivot_kolli.sort_values("Totalt kolli", ascending=False)
        country_table["Kolli"] = kolli_sorted["Totalt kolli"].values

        # Add cost-per-kolli columns
        for typ in type_order:
            kolli_col = kolli_sorted[typ].values
            amount_col = country_table[typ].values
            country_table[f"Snitt {typ.lower()}"] = [
                round(a / k, 1) if k > 0 else 0.0
                for a, k in zip(amount_col, kolli_col)
            ]
        total_kolli = kolli_sorted["Totalt kolli"].values
        country_table["Snitt totalt"] = [
            round(a / k, 1) if k > 0 else 0.0
            for a, k in zip(country_table["Totalt"].values, total_kolli)
        ]
    else:
        country_table = (
            df.groupby("Land")
            .agg(Kolli=("Kolli", "sum"), **{"Totalt": ("Belopp (SEK)", "sum")})
            .sort_values("Totalt", ascending=False)
            .reset_index()
        )
        country_table["Andel"] = (country_table["Totalt"] / total_amount * 100).round(1)
        country_table["Snitt totalt"] = (country_table["Totalt"] / country_table["Kolli"]).round(1)

    # ── Add "Ej allokerat" row if invoice total is known ─────────────────
    parsed_total = country_table["Totalt"].sum()
    if invoice_total and invoice_total > parsed_total + 1:
        gap = round(invoice_total - parsed_total, 2)
        gap_row = {"Land": "Ej allokerat", "Totalt": gap, "Kolli": 0,
                   "Andel": round(gap / invoice_total * 100, 1),
                   "Snitt totalt": 0.0}
        if has_types:
            for typ in type_order:
                gap_row[typ] = 0.0
                gap_row[f"Snitt {typ.lower()}"] = 0.0
        country_table = pd.concat(
            [country_table, pd.DataFrame([gap_row])], ignore_index=True
        )
        # Recalculate Andel based on invoice total
        country_table["Andel"] = (
            country_table["Totalt"] / invoice_total * 100
        ).round(1)

    # ── Stacked bar chart ────────────────────────────────────────────────
    chart_col1, chart_col2 = st.columns([3, 2])

    with chart_col1:
        if has_types:
            chart_data = country_table.sort_values("Totalt", ascending=True)
            fig_bar = px.bar(
                chart_data.melt(
                    id_vars=["Land"],
                    value_vars=type_order,
                    var_name="Typ",
                    value_name="SEK",
                ),
                x="SEK", y="Land", color="Typ", orientation="h",
                color_discrete_map={
                    "Utgående": "#2171b5",
                    "Retur": "#cb4b16",
                    "Övrigt": "#999999",
                },
                text="SEK",
            )
            fig_bar.update_traces(
                texttemplate="%{text:,.0f}", textposition="inside", textfont_size=10,
            )
            fig_bar.update_layout(
                title="Kostnad per land & typ (SEK)",
                xaxis_title="", yaxis_title="",
                barmode="stack",
                height=max(400, n_countries * 30 + 100),
                margin=dict(l=10, r=20, t=40, b=20),
                font=dict(family="DM Sans, sans-serif"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
        else:
            fig_bar = px.bar(
                country_table.sort_values("Totalt", ascending=True),
                x="Totalt", y="Land", orientation="h",
                text="Totalt", color="Totalt",
                color_continuous_scale=["#c6dbef", "#2171b5"],
            )
            fig_bar.update_traces(
                texttemplate="%{text:,.0f}", textposition="outside", textfont_size=11,
            )
            fig_bar.update_layout(
                title="Kostnad per land (SEK)",
                xaxis_title="", yaxis_title="",
                coloraxis_showscale=False,
                height=max(400, n_countries * 30 + 100),
                margin=dict(l=10, r=80, t=40, b=20),
                font=dict(family="DM Sans, sans-serif"),
            )
        st.plotly_chart(fig_bar, use_container_width=True)

    with chart_col2:
        top_n = 8
        totals_col = "Totalt"
        if len(country_table) > top_n:
            top = country_table.head(top_n).copy()
            other_sum = country_table.iloc[top_n:][totals_col].sum()
            other_row = pd.DataFrame([{"Land": "Övriga", totals_col: other_sum}])
            pie_data = pd.concat(
                [top[["Land", totals_col]], other_row], ignore_index=True
            )
        else:
            pie_data = country_table[["Land", totals_col]].copy()

        fig_pie = px.pie(
            pie_data, values=totals_col, names="Land",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_pie.update_traces(
            textposition="inside", textinfo="label+percent", textfont_size=11,
        )
        fig_pie.update_layout(
            title="Andel per land", showlegend=False, height=450,
            margin=dict(l=10, r=10, t=40, b=20),
            font=dict(family="DM Sans, sans-serif"),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # ── Table ────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Detaljerad tabell")

    display_df = country_table.copy()
    # Format number columns
    for col in display_df.columns:
        if col in ("Land",):
            continue
        if col == "Andel":
            display_df[col] = display_df[col].map("{:.1f}%".format)
        elif col == "Kolli":
            display_df[col] = display_df[col].astype(int)
        else:
            display_df[col] = display_df[col].map(
                lambda x: "{:,.0f}".format(x) if isinstance(x, (int, float)) else x
            )

    st.dataframe(
        display_df, use_container_width=True, hide_index=True,
        column_config={
            "Land": st.column_config.TextColumn("Land", width="medium"),
        },
    )

    # ── Downloads ────────────────────────────────────────────────────────
    st.markdown("---")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        csv_summary = country_table.to_csv(index=False, sep=";", decimal=",")
        st.download_button(
            "📥 Ladda ner sammanfattning (CSV)",
            csv_summary,
            file_name=f"shipsplit_per_land_{carrier.lower()}.csv",
            mime="text/csv",
        )

    with dl_col2:
        csv_detail = df.to_csv(index=False, sep=";", decimal=",")
        st.download_button(
            "📥 Ladda ner alla rader (CSV)",
            csv_detail,
            file_name=f"shipsplit_detalj_{carrier.lower()}.csv",
            mime="text/csv",
        )

    # ── Expandable: raw data ─────────────────────────────────────────────
    with st.expander(f"Visa alla {n_lines} rader"):
        st.dataframe(
            df.sort_values("Belopp (SEK)", ascending=False),
            use_container_width=True, hide_index=True,
        )

    # ── Footer ───────────────────────────────────────────────────────────
    if invoice_total and invoice_total > parsed_total + 1:
        st.caption(
            f"ℹ️ Fakturans totalbelopp exkl. moms: **{invoice_total:,.0f} SEK**. "
            f"Allokerat till sändningar: {parsed_total:,.0f} SEK ({parsed_total/invoice_total*100:.1f}%). "
            f"\"Ej allokerat\" ({invoice_total - parsed_total:,.0f} SEK) avser adressändringar, "
            f"korrigeringar, upphämtningsavgifter eller andra poster utan landskoppling."
        )
    elif invoice_total:
        st.caption(
            f"ℹ️ Fakturans totalbelopp exkl. moms: {invoice_total:,.0f} SEK — "
            f"100% allokerat till sändningar."
        )

else:
    st.markdown("""
    ### Så här fungerar det
    1. **Ladda upp** en fraktfaktura i PDF-format
    2. **Automatisk analys** — systemet identifierar fraktbolaget och parsar alla rader
    3. **Se resultatet** — kostnad per land med diagram och nedladdningsbar CSV

    Stödjer för närvarande:
    - ✅ **UPS** — fullständigt stöd (svenska fakturor)
    - ✅ **Bring** — fullständigt stöd
    - ✅ **DHL Freight** — inrikes (identifieras korrekt)
    - 🔧 **DHL Express** — under utveckling
    """)
