"""
Shipping Invoice Analyzer — Babyshop
Parses UPS (and DHL) shipping invoices and breaks down costs by country.
"""

import streamlit as st
import pdfplumber
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import re
import io
from collections import defaultdict

# ─── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Shipping Cost Analyzer",
    page_icon="📦",
    layout="wide",
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }
    .block-container {
        padding-top: 2rem;
        max-width: 1100px;
    }
    h1 { font-weight: 700; letter-spacing: -0.02em; }
    h2, h3 { font-weight: 600; }

    /* Metric cards */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        border: 1px solid #dee2e6;
        border-radius: 12px;
        padding: 1rem 1.25rem;
    }
    div[data-testid="stMetric"] label {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: #6c757d;
    }
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        font-weight: 700;
        font-size: 1.6rem;
    }

    /* Upload area */
    [data-testid="stFileUploader"] {
        border: 2px dashed #adb5bd;
        border-radius: 12px;
        padding: 1rem;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: #495057;
    }
</style>
""", unsafe_allow_html=True)


# ─── Country name normalization ─────────────────────────────────────────────
COUNTRY_NORMALIZE = {
    # Swedish UPS names → clean display names
    "IRLÄNDSKA": "Irland",
    "FÖRENADE": "Förenade Arabemiraten",
    "SLOVAKISKA": "Slovakien",
    "TJECKISKA": "Tjeckien",
    "STORBRITANNIEN": "Storbritannien",
    "NEDERLÄNDERNA": "Nederländerna",
    "TYSKLAND": "Tyskland",
    "SYDKOREA": "Sydkorea",
    "SPANIEN": "Spanien",
    "ITALIEN": "Italien",
    "FRANKRIKE": "Frankrike",
    "BELGIEN": "Belgien",
    "GREKLAND": "Grekland",
    "SLOVENIEN": "Slovenien",
    "LITAUEN": "Litauen",
    "PORTUGAL": "Portugal",
    "LUXEMBURG": "Luxemburg",
    "KROATIEN": "Kroatien",
    "ÖSTERRIKE": "Österrike",
    "UNGERN": "Ungern",
    "RUMÄNIEN": "Rumänien",
    "SCHWEIZ": "Schweiz",
    "ISLAND": "Island",
    "ISRAEL": "Israel",
    "ESTLAND": "Estland",
    "POLEN": "Polen",
    "KANADA": "Kanada",
    "KAZAKHSTAN": "Kazakstan",
    "TAIWAN": "Taiwan",
    "MALTA": "Malta",
    "USA": "USA",
    "JAPAN": "Japan",
    "SVERIGE": "Sverige",
    "LETTLAND": "Lettland",
    "BULGARIEN": "Bulgarien",
    "CYPERN": "Cypern",
    "FINLAND": "Finland",
    "NORGE": "Norge",
    "DANMARK": "Danmark",
    "KINA": "Kina",
    "AUSTRALIEN": "Australien",
    "SINGAPORE": "Singapore",
    "TURKIET": "Turkiet",
    "INDIEN": "Indien",
    "MEXIKO": "Mexiko",
    "BRASILIEN": "Brasilien",
    "HONGKONG": "Hongkong",
    "THAILAND": "Thailand",
    "VIETNAM": "Vietnam",
    "FILIPPINERNA": "Filippinerna",
    "MALAYSIA": "Malaysia",
    "INDONESIEN": "Indonesien",
}


def normalize_country(raw: str) -> str:
    """Normalize a raw country string from invoice to clean display name."""
    raw = raw.strip().upper()
    if raw in COUNTRY_NORMALIZE:
        return COUNTRY_NORMALIZE[raw]
    # Fallback: title-case
    return raw.title()


def parse_swedish_number(s: str) -> float:
    """Parse Swedish-formatted number: dots as thousands, comma as decimal."""
    s = s.strip()
    # Remove dots (thousand separators), replace comma with period
    s = s.replace(".", "").replace(",", ".")
    return float(s)


# ─── UPS Parser ─────────────────────────────────────────────────────────────

def detect_carrier(text: str) -> str:
    """Detect which carrier the invoice is from."""
    lower = text[:3000].lower()
    if "ups" in lower and ("sändning" in lower or "express saver" in lower):
        return "UPS"
    if "dhl" in lower and ("waybill" in lower or "fraktsedel" in lower or "shipment" in lower):
        return "DHL"
    return "Unknown"


def parse_ups_invoice(pdf_file) -> pd.DataFrame:
    """
    Parse a UPS shipping invoice PDF.
    Uses the Specification sections which have:
      - Mottagare: ... COUNTRY
      - Total kostnad för sändning TRACKING SEK Belopp Rabatt Nettoavg
    """
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")

    # Patterns
    mottagare_re = re.compile(r"Mottagare:\s+.+\s(\S+)\s*$")
    total_re = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)"
    )
    # Fallback: only 2 amounts (no discount column)
    total_re_2 = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s*$"
    )
    date_re = re.compile(r"^(\d{2}\s+\w{3})\s+")

    records = []
    current_country = None
    current_date = None

    for line in lines:
        # Track dates
        dm = date_re.match(line)
        if dm and ("1Z" in line or "Express" in line or "Standard" in line or "Expedited" in line):
            current_date = dm.group(1).strip()

        # Mottagare line → extract country
        mm = mottagare_re.search(line)
        if mm:
            current_country = normalize_country(mm.group(1))
            continue

        # Total cost line → extract amounts
        if "Total kostnad för sändning" in line and current_country:
            tm = total_re.search(line)
            if tm:
                tracking = tm.group(1)
                gross = parse_swedish_number(tm.group(2))
                discount = parse_swedish_number(tm.group(3))
                net = parse_swedish_number(tm.group(4))
                records.append({
                    "Tracking": tracking,
                    "Land": current_country,
                    "Datum": current_date or "",
                    "Brutto (SEK)": gross,
                    "Rabatt (SEK)": discount,
                    "Netto (SEK)": net,
                })
                current_country = None
                continue

            tm2 = total_re_2.search(line)
            if tm2:
                tracking = tm2.group(1)
                gross = parse_swedish_number(tm2.group(2))
                net = parse_swedish_number(tm2.group(3))
                records.append({
                    "Tracking": tracking,
                    "Land": current_country,
                    "Datum": current_date or "",
                    "Brutto (SEK)": gross,
                    "Rabatt (SEK)": 0.0,
                    "Netto (SEK)": net,
                })
                current_country = None

    return pd.DataFrame(records)


def parse_dhl_invoice(pdf_file) -> pd.DataFrame:
    """
    Parse a DHL shipping invoice PDF.
    Placeholder — extend with actual DHL format parsing.
    """
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")

    # DHL invoices often have: Receiver Country, Charged Weight, Total Amount
    # This is a starter parser — needs refinement per actual DHL format
    # Common patterns in DHL Express invoices:
    #   - "Destination: COUNTRY" or country code columns
    #   - Per-waybill totals

    # Try to find waybill-level data
    records = []
    # Patterns for DHL — these need tuning to actual format
    waybill_re = re.compile(r"(\d{10,})")  # DHL waybill numbers are 10+ digits
    country_re = re.compile(r"(?:Destination|Mottagarland|Country)[\s:]+([A-Z]{2,})", re.IGNORECASE)

    st.warning(
        "⚠️ DHL-parsern är i beta. Resultaten kan behöva manuell verifiering. "
        "Ladda gärna upp en exempelfaktura så kan vi finjustera parsern."
    )

    return pd.DataFrame(records)


# ─── Main UI ────────────────────────────────────────────────────────────────

st.title("📦 Shipping Invoice Analyzer")
st.markdown("Ladda upp en fraktfaktura (PDF) från **UPS** eller **DHL** för att se hur kostnaden fördelas per land.")

uploaded_file = st.file_uploader(
    "Välj PDF-faktura",
    type=["pdf"],
    help="Stödjer UPS-fakturor (svenska). DHL-stöd under utveckling.",
)

if uploaded_file is not None:
    # Detect carrier
    with pdfplumber.open(uploaded_file) as pdf:
        sample_text = ""
        for page in pdf.pages[:3]:
            t = page.extract_text()
            if t:
                sample_text += t + "\n"

    carrier = detect_carrier(sample_text)
    uploaded_file.seek(0)  # Reset file pointer

    if carrier == "Unknown":
        st.error("❌ Kunde inte identifiera fraktbolaget. Stödjer för närvarande UPS och DHL.")
        st.stop()

    st.info(f"🔍 Identifierat: **{carrier}**-faktura. Analyserar...")

    # Parse
    with st.spinner("Extraherar data från PDF..."):
        if carrier == "UPS":
            df = parse_ups_invoice(uploaded_file)
        elif carrier == "DHL":
            uploaded_file.seek(0)
            df = parse_dhl_invoice(uploaded_file)
        else:
            df = pd.DataFrame()

    if df.empty:
        st.warning("Inga sändningar hittades i fakturan.")
        st.stop()

    # ── Summary metrics ──────────────────────────────────────────────────
    st.markdown("---")
    total_net = df["Netto (SEK)"].sum()
    total_gross = df["Brutto (SEK)"].sum()
    total_discount = df["Rabatt (SEK)"].sum()
    n_shipments = len(df)
    n_countries = df["Land"].nunique()
    avg_per_shipment = total_net / n_shipments if n_shipments > 0 else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Totalt netto", f"{total_net:,.0f} SEK")
    col2.metric("Sändningar", f"{n_shipments:,}")
    col3.metric("Länder", f"{n_countries}")
    col4.metric("Snitt / sändning", f"{avg_per_shipment:,.0f} SEK")

    # ── Country breakdown ────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Fördelning per land")

    country_agg = (
        df.groupby("Land")
        .agg(
            Sändningar=("Tracking", "count"),
            **{"Brutto (SEK)": ("Brutto (SEK)", "sum")},
            **{"Rabatt (SEK)": ("Rabatt (SEK)", "sum")},
            **{"Netto (SEK)": ("Netto (SEK)", "sum")},
        )
        .sort_values("Netto (SEK)", ascending=False)
        .reset_index()
    )
    country_agg["Andel"] = (country_agg["Netto (SEK)"] / total_net * 100).round(1)
    country_agg["Snitt / sändning (SEK)"] = (
        country_agg["Netto (SEK)"] / country_agg["Sändningar"]
    ).round(0)

    # ── Charts ───────────────────────────────────────────────────────────
    chart_col1, chart_col2 = st.columns([3, 2])

    with chart_col1:
        # Horizontal bar chart — net cost by country
        fig_bar = px.bar(
            country_agg.sort_values("Netto (SEK)", ascending=True),
            x="Netto (SEK)",
            y="Land",
            orientation="h",
            text="Netto (SEK)",
            color="Netto (SEK)",
            color_continuous_scale=["#c6dbef", "#2171b5"],
        )
        fig_bar.update_traces(
            texttemplate="%{text:,.0f}",
            textposition="outside",
            textfont_size=11,
        )
        fig_bar.update_layout(
            title="Nettokostnad per land (SEK)",
            xaxis_title="",
            yaxis_title="",
            coloraxis_showscale=False,
            height=max(400, n_countries * 30 + 100),
            margin=dict(l=10, r=80, t=40, b=20),
            font=dict(family="DM Sans, sans-serif"),
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with chart_col2:
        # Pie chart — top 8 + "Övriga"
        top_n = 8
        if len(country_agg) > top_n:
            top = country_agg.head(top_n).copy()
            other_sum = country_agg.iloc[top_n:]["Netto (SEK)"].sum()
            other_row = pd.DataFrame(
                [{"Land": "Övriga", "Netto (SEK)": other_sum}]
            )
            pie_data = pd.concat([top[["Land", "Netto (SEK)"]], other_row], ignore_index=True)
        else:
            pie_data = country_agg[["Land", "Netto (SEK)"]].copy()

        fig_pie = px.pie(
            pie_data,
            values="Netto (SEK)",
            names="Land",
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig_pie.update_traces(
            textposition="inside",
            textinfo="label+percent",
            textfont_size=11,
        )
        fig_pie.update_layout(
            title="Andel per land",
            showlegend=False,
            height=450,
            margin=dict(l=10, r=10, t=40, b=20),
            font=dict(family="DM Sans, sans-serif"),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # ── Table ────────────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Detaljerad tabell")

    # Format for display
    display_df = country_agg.copy()
    display_df["Brutto (SEK)"] = display_df["Brutto (SEK)"].map("{:,.0f}".format)
    display_df["Rabatt (SEK)"] = display_df["Rabatt (SEK)"].map("{:,.0f}".format)
    display_df["Netto (SEK)"] = display_df["Netto (SEK)"].map("{:,.0f}".format)
    display_df["Andel"] = display_df["Andel"].map("{:.1f}%".format)
    display_df["Snitt / sändning (SEK)"] = display_df["Snitt / sändning (SEK)"].map("{:,.0f}".format)

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Land": st.column_config.TextColumn("Land", width="medium"),
            "Sändningar": st.column_config.NumberColumn("Sändningar", width="small"),
        },
    )

    # ── Downloads ────────────────────────────────────────────────────────
    st.markdown("---")
    dl_col1, dl_col2 = st.columns(2)

    with dl_col1:
        # Download country summary as CSV
        csv_summary = country_agg.to_csv(index=False, sep=";", decimal=",")
        st.download_button(
            "📥 Ladda ner sammanfattning (CSV)",
            csv_summary,
            file_name=f"fraktanalys_per_land_{carrier.lower()}.csv",
            mime="text/csv",
        )

    with dl_col2:
        # Download full shipment detail as CSV
        csv_detail = df.to_csv(index=False, sep=";", decimal=",")
        st.download_button(
            "📥 Ladda ner alla sändningar (CSV)",
            csv_detail,
            file_name=f"fraktanalys_detalj_{carrier.lower()}.csv",
            mime="text/csv",
        )

    # ── Expandable: raw shipment data ────────────────────────────────────
    with st.expander(f"Visa alla {n_shipments} sändningar"):
        st.dataframe(
            df.sort_values("Netto (SEK)", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    # ── Footer note ──────────────────────────────────────────────────────
    st.caption(
        f"ℹ️ Analysen baseras på specifikationssektionerna i fakturan. "
        f"Eventuella adressändringar, korrigeringar och justeringar som inte "
        f"är knutna till en specifik sändning ingår inte i landsfördelningen."
    )

else:
    # Landing state
    st.markdown("""
    ### Så här fungerar det
    1. **Ladda upp** en fraktfaktura i PDF-format
    2. **Automatisk analys** — systemet identifierar fraktbolaget och parsar alla sändningar
    3. **Se resultatet** — kostnad per land med diagram och nedladdningsbar CSV

    Stödjer för närvarande:
    - ✅ **UPS** — fullständigt stöd (svenska fakturor)
    - 🔧 **DHL** — under utveckling
    """)
