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

    mottagare_re = re.compile(r"Mottagare:\s+.+\s(\S+)\s*$")
    total_re = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)"
    )
    total_re_2 = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s*$"
    )

    records = []
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
                    "Detalj": tm2.group(1),
                })
                current_country = None

    return pd.DataFrame(records)


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
                "Detalj": service,
            })

    return pd.DataFrame(records)


# ─── DHL Parser (stub) ─────────────────────────────────────────────────────

def parse_dhl_freight_invoice(pdf_file) -> pd.DataFrame:
    st.info(
        "ℹ️ DHL Freight-faktura identifierad. Denna faktura innehåller enbart "
        "inrikes sändningar (Sverige). Landsfördelning visar 100% Sverige."
    )
    # All DHL Freight Lekmera invoices are domestic SE → SE
    # Still parse the TOTALT amounts from specification pages for a useful summary
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")

    # Parse TOTALT amounts from specification pages (rightmost number on D-lines)
    total_re = re.compile(r"([\d,]+)\s*$")
    records = []
    current_service = None
    current_city = None

    for line in lines:
        stripped = line.strip()
        # A-line: tracking + service + origin city
        if stripped.startswith("A ") and ("SERVPOINT" in stripped or "HOME DELIVERY" in stripped):
            if "SERVPOINT B2C" in stripped:
                current_service = "Servpoint B2C"
            elif "SERVPOINT C2B" in stripped:
                current_service = "Servpoint C2B"
            elif "HOME DELIVERY" in stripped:
                current_service = "Home Delivery"
            else:
                current_service = "Övrigt"
            # City is after the service name
            parts = stripped.split("JÖNKÖPING")
            if len(parts) > 1:
                # weight is at end, city comes after JÖNKÖPING on B-line
                pass
        # B-line: has destination city
        if stripped.startswith("B ") and current_service:
            parts = stripped.split()
            if len(parts) >= 2:
                current_city = parts[1]  # city name

        # TOTALT amount line (appears after D-line with km distance)
        if current_service and current_city:
            m = re.search(r"(\d+,\d{2})\s*$", stripped)
            if m and not stripped.startswith(("A ", "B ", "C ", "D ")):
                amount = float(m.group(1).replace(",", "."))
                if amount > 5:  # filter out tiny noise
                    records.append({
                        "Land": "Sverige",
                        "Belopp (SEK)": amount,
                        "Kolli": 1,
                        "Detalj": f"{current_service} → {current_city}",
                    })
                    current_service = None
                    current_city = None

    return pd.DataFrame(records)


def parse_dhl_express_invoice(pdf_file) -> pd.DataFrame:
    st.warning(
        "⚠️ DHL Express-parsern är inte implementerad än. "
        "Ladda upp en exempelfaktura så bygger vi stöd för den."
    )
    return pd.DataFrame()


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
        if carrier == "UPS":
            df = parse_ups_invoice(uploaded_file)
        elif carrier == "Bring":
            df = parse_bring_invoice(uploaded_file)
        elif carrier == "DHL Freight":
            df = parse_dhl_freight_invoice(uploaded_file)
        elif carrier == "DHL Express":
            df = parse_dhl_express_invoice(uploaded_file)
        elif carrier == "DHL":
            df = parse_dhl_express_invoice(uploaded_file)
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

    # ── Country breakdown ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Fördelning per land")

    country_agg = (
        df.groupby("Land")
        .agg(
            Kolli=("Kolli", "sum"),
            Rader=("Belopp (SEK)", "count"),
            **{"Belopp (SEK)": ("Belopp (SEK)", "sum")},
        )
        .sort_values("Belopp (SEK)", ascending=False)
        .reset_index()
    )
    country_agg["Andel"] = (country_agg["Belopp (SEK)"] / total_amount * 100).round(1)
    country_agg["Snitt / kolli (SEK)"] = (
        country_agg["Belopp (SEK)"] / country_agg["Kolli"]
    ).round(1)

    # ── Charts ───────────────────────────────────────────────────────────
    chart_col1, chart_col2 = st.columns([3, 2])

    with chart_col1:
        fig_bar = px.bar(
            country_agg.sort_values("Belopp (SEK)", ascending=True),
            x="Belopp (SEK)", y="Land", orientation="h",
            text="Belopp (SEK)", color="Belopp (SEK)",
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
        if len(country_agg) > top_n:
            top = country_agg.head(top_n).copy()
            other_sum = country_agg.iloc[top_n:]["Belopp (SEK)"].sum()
            other_row = pd.DataFrame([{"Land": "Övriga", "Belopp (SEK)": other_sum}])
            pie_data = pd.concat(
                [top[["Land", "Belopp (SEK)"]], other_row], ignore_index=True
            )
        else:
            pie_data = country_agg[["Land", "Belopp (SEK)"]].copy()

        fig_pie = px.pie(
            pie_data, values="Belopp (SEK)", names="Land",
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

    display_df = country_agg.copy()
    display_df["Belopp (SEK)"] = display_df["Belopp (SEK)"].map("{:,.0f}".format)
    display_df["Andel"] = display_df["Andel"].map("{:.1f}%".format)
    display_df["Snitt / kolli (SEK)"] = display_df["Snitt / kolli (SEK)"].map("{:,.1f}".format)

    st.dataframe(
        display_df, use_container_width=True, hide_index=True,
        column_config={
            "Land": st.column_config.TextColumn("Land", width="medium"),
            "Kolli": st.column_config.NumberColumn("Kolli", width="small"),
            "Rader": st.column_config.NumberColumn("Fakturarader", width="small"),
        },
    )

    # ── Downloads ────────────────────────────────────────────────────────
    st.markdown("---")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        csv_summary = country_agg.to_csv(index=False, sep=";", decimal=",")
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
    if carrier == "UPS":
        st.caption(
            "ℹ️ Analysen baseras på specifikationssektionerna i fakturan. "
            "Adressändringar, korrigeringar och justeringar som inte är knutna "
            "till en specifik sändning ingår inte i landsfördelningen."
        )
    elif carrier == "Bring":
        st.caption(
            "ℹ️ Alla fakturarader ingår i analysen. Kundlandet bestäms av "
            "destinationsland för utgående sändningar och avsändarland för returer."
        )
    elif carrier == "DHL Freight":
        st.caption(
            "ℹ️ DHL Freight — inrikes sändningar. Alla leveranser är inom Sverige."
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
