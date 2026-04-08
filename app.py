"""
Shipsplit — Shipping Invoice Analyzer
Parses shipping invoices (UPS, Bring, DHL) and breaks down costs by country.
Stores results in Supabase for history and time-series analysis.
"""

import streamlit as st
import pdfplumber
import pandas as pd
import plotly.express as px
import re
from datetime import datetime, date, timedelta

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


# ═══════════════════════════════════════════════════════════════════════════════
# PARSING LOGIC (unchanged from working version)
# ═══════════════════════════════════════════════════════════════════════════════

COUNTRY_NAME_MAP = {
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
    # Guard: if raw is all digits, it's a postal code, not a country
    if raw.isdigit():
        return "Okänt"
    return COUNTRY_NAME_MAP.get(raw, raw.title())


def parse_swedish_number(s: str) -> float:
    return float(s.strip().replace(" ", "").replace(".", "").replace(",", "."))


def detect_carrier(text: str) -> str:
    lower = text[:5000].lower()
    if "ups" in lower and ("sändning" in lower or "express saver" in lower
                           or "ups returns" in lower or "totalkostnad" in lower):
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


def parse_ups_invoice(pdf_file):
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")
    records = []

    # Format 1: Outbound (Mottagare → Total kostnad för sändning)
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
                    "Land": current_country, "Belopp (SEK)": parse_swedish_number(tm.group(4)),
                    "Kolli": 1, "Typ": "Utgående", "Detalj": tm.group(1),
                })
                current_country = None
                continue
            tm2 = total_re_2.search(line)
            if tm2:
                records.append({
                    "Land": current_country, "Belopp (SEK)": parse_swedish_number(tm2.group(3)),
                    "Kolli": 1, "Typ": "Utgående", "Detalj": tm2.group(1),
                })
                current_country = None

    # Format 2: Returns (Skickat från / Avsändare → Totalkostnad)
    skickat_re = re.compile(r"Skickat från:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    avsandare_re = re.compile(r"Avsändare:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    totalkostnad_re = re.compile(
        r"Totalkostnad\s+SEK\s+[\d.,]+\s+(?:[\d.,]+\s+)?[\d.,]+\s+([\d.,]+)\s*$"
    )
    current_country = None
    current_section = None

    for line in lines:
        s = line.strip()
        if "UPS Returns" in s:
            current_section = "Retur"
        elif "importsändningar" in s.lower():
            current_section = "Retur"
        elif "icke levererbara returer" in s.lower():
            current_section = "Retur"
        elif "upphämtningsbegäran" in s.lower():
            current_section = "Övrigt"

        sm = skickat_re.search(line)
        if sm:
            current_country = normalize_country(sm.group(1))
            continue
        am = avsandare_re.search(line)
        if am:
            current_country = normalize_country(am.group(1))
            continue
        if "Totalkostnad" in line and "SEK" in line and current_country:
            tm = totalkostnad_re.search(line)
            if tm:
                records.append({
                    "Land": current_country, "Belopp (SEK)": parse_swedish_number(tm.group(1)),
                    "Kolli": 1, "Typ": current_section or "Retur",
                    "Detalj": current_section or "Retur",
                })
                current_country = None

    # Invoice total
    invoice_total = None
    momspliktigt = icke_moms = 0.0
    for line in lines:
        m = re.search(r"Totalt momspliktigt\s+([\d.,]+)", line)
        if m:
            momspliktigt = parse_swedish_number(m.group(1))
        m2 = re.search(r"Icke momspliktigt\s+([\d.,]+)", line)
        if m2:
            icke_moms = parse_swedish_number(m2.group(1))
    if momspliktigt > 0 or icke_moms > 0:
        invoice_total = momspliktigt + icke_moms

    # Extract overhead charges
    overhead = []
    privatadress_total = 0.0
    sasong_total = 0.0
    adress_total = 0.0
    justeringar_total = 0.0

    for line in lines:
        # Residential delivery surcharge
        m = re.search(r"Totala justeringar för leveverans till privatadresser\s+SEK\s+([\d.,]+)\s+([\d.,]+)", line)
        if m:
            privatadress_total += parse_swedish_number(m.group(2))

        # Seasonal surcharge
        m = re.search(r"Totalkostnad för just\. av säsongsbas\. tilläggsavg\.\s+SEK\s+([\d.,]+)\s+([\d.,]+)", line)
        if m:
            sasong_total += parse_swedish_number(m.group(2))

        # Address corrections
        m = re.search(r"Total kostnad för adressändring\s+SEK\s+([\d.,]+)\s+([\d.,]+)", line)
        if m:
            adress_total += parse_swedish_number(m.group(2))

        # Total adjustments (includes all sub-categories)
        m = re.search(r"Totala justeringar\s+SEK\s+([\d.,]+)\s+([\d.,]+)", line)
        if m:
            justeringar_total += parse_swedish_number(m.group(2))

    # Weight corrections = total justeringar minus known sub-categories
    vikt_total = max(0, justeringar_total - privatadress_total - sasong_total)

    if privatadress_total > 0:
        overhead.append({"Kategori": "Leverans till privatadress", "Belopp (SEK)": privatadress_total})
    if vikt_total > 0:
        overhead.append({"Kategori": "Viktkorrigeringar", "Belopp (SEK)": vikt_total})
    if sasong_total > 0:
        overhead.append({"Kategori": "Säsongsbaserat tillägg", "Belopp (SEK)": sasong_total})
    if adress_total > 0:
        overhead.append({"Kategori": "Adressändring", "Belopp (SEK)": adress_total})

    return pd.DataFrame(records), invoice_total, overhead


def parse_bring_invoice(pdf_file):
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")
    line_re = re.compile(
        r"^(\d+)\s+\d+\s+\d+\s+(.+?)\s+([A-Z]{2})\s+([A-Z]{2})\s+(\d+)\s+St\s+"
        r"([\d,]+)\s+(?:[\d.]+\s+)?(?:Export|Local VAT)\s+([\d\s,]+)$"
    )
    records = []
    for line in lines:
        line = line.strip()
        m = line_re.match(line)
        if m:
            service = m.group(2).strip()
            from_c, to_c = m.group(3), m.group(4)
            qty = int(m.group(5))
            amount = float(m.group(7).replace(" ", "").replace(",", "."))

            # Surcharge lines (Fuel fee, Veiavgift, Label Free, etc.) have a comma
            # in the service name. These apply to already-counted parcels, so kolli=0.
            is_surcharge = "," in service
            kolli = 0 if is_surcharge else qty

            # "Attempted Delivery Return" is a failed outbound delivery, not a customer return
            is_return = "Return" in service and "Attempted Delivery" not in service

            if from_c == to_c:
                customer = from_c
            elif is_return:
                customer = from_c if from_c != "SE" else to_c
            else:
                customer = to_c if to_c != "SE" else from_c
            records.append({
                "Land": normalize_country(customer), "Belopp (SEK)": amount,
                "Kolli": kolli, "Typ": "Retur" if is_return else "Utgående",
                "Detalj": service,
            })

    invoice_total = None
    m = re.search(r"Summa exkl\.?\s*moms\s+([\d\s.,]+)", full_text)
    if m:
        invoice_total = parse_swedish_number(m.group(1))
    return pd.DataFrame(records), invoice_total, []


def parse_dhl_freight_invoice(pdf_file):
    with pdfplumber.open(pdf_file) as pdf:
        full_text = ""
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                full_text += t + "\n"

    lines = full_text.split("\n")
    records = []
    current_service = current_city = None

    for line in lines:
        s = line.strip()
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
        elif re.match(r"^B\s+\d", s) and current_service:
            parts = s.split()
            if len(parts) >= 3:
                current_city = parts[2]
        elif re.match(r"^D\s", s) and current_service and "Tilläggs" not in s:
            m = re.search(r"(\d+)\s+(\d+)\s+(\d+,\d{2})\s*$", s)
            if m:
                records.append({
                    "Land": "Sverige", "Belopp (SEK)": float(m.group(3).replace(",", ".")),
                    "Kolli": int(m.group(2)),
                    "Typ": "Retur" if current_service == "Servpoint C2B" else "Utgående",
                    "Detalj": f"{current_service} → {current_city or '?'}",
                })
                current_service = current_city = None

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
                "Land": "Sverige", "Belopp (SEK)": amount, "Kolli": count,
                "Typ": "Retur" if "C2B" in m.group(1).upper() else "Utgående",
                "Detalj": service,
            })

    invoice_total = None
    m = re.search(r"Summa exkl\.?\s*moms\s+([\d\s.,]+)", full_text)
    if m:
        invoice_total = parse_swedish_number(m.group(1))
    else:
        m2 = re.search(r"Momspliktigt belopp\s+SEK\s+([\d\s.,]+)", full_text)
        if m2:
            invoice_total = parse_swedish_number(m2.group(1))

    return pd.DataFrame(records), invoice_total, []


def parse_dhl_express_invoice(pdf_file):
    return pd.DataFrame(), None, []


def extract_invoice_dates(pdf_file):
    """Extract invoice date and order period from a PDF invoice."""
    with pdfplumber.open(pdf_file) as pdf:
        text = ""
        for p in pdf.pages[:2]:
            t = p.extract_text()
            if t:
                text += t + "\n"

    dates = {"invoice_date": None, "period_start": None, "period_end": None}
    date_re = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

    # Bring: "Orderperiod 2026-03-16 - 2026-03-22"
    m = re.search(r"Orderperiod\s+(\d{4}-\d{2}-\d{2})\s*-\s*(\d{4}-\d{2}-\d{2})", text)
    if m:
        dates["period_start"] = m.group(1)
        dates["period_end"] = m.group(2)

    # Bring: "Fakturadatum 2026-03-23"
    m = re.search(r"Fakturadatum\s+(\d{4}-\d{2}-\d{2})", text)
    if m:
        dates["invoice_date"] = m.group(1)

    # UPS: "Fakturadatum\n17 mars 2026"
    if not dates["invoice_date"]:
        months_sv = {"januari": "01", "februari": "02", "mars": "03", "april": "04",
                     "maj": "05", "juni": "06", "juli": "07", "augusti": "08",
                     "september": "09", "oktober": "10", "november": "11", "december": "12"}
        m = re.search(r"Fakturadatum\s*\n?\s*(\d{1,2})\s+(\w+)\s+(\d{4})", text)
        if m:
            day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
            if month_name in months_sv:
                dates["invoice_date"] = f"{year}-{months_sv[month_name]}-{int(day):02d}"

    # DHL: "Fakturadatum 2026-03-14" or "Fakturadatum: 20260314"
    if not dates["invoice_date"]:
        m = re.search(r"Fakturadatum:?\s*(\d{4})(\d{2})(\d{2})", text)
        if m:
            dates["invoice_date"] = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # If no period found, use invoice date as both
    if not dates["period_start"] and dates["invoice_date"]:
        dates["period_start"] = dates["invoice_date"]
        dates["period_end"] = dates["invoice_date"]

    return dates


def check_password():
    """Simple password gate. Returns True if authenticated."""
    try:
        correct_pw = st.secrets["auth"]["password"]
    except (KeyError, FileNotFoundError):
        return True  # No password configured, allow access

    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if st.session_state.authenticated:
        return True

    st.title("📦 Shipsplit")
    pw = st.text_input("Lösenord", type="password")
    if pw:
        if pw == correct_pw:
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Fel lösenord.")
    else:
        st.info("Ange lösenord för att fortsätta.")
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE LAYER (Supabase)
# ═══════════════════════════════════════════════════════════════════════════════

def init_supabase():
    """Initialize Supabase client from Streamlit secrets."""
    try:
        from supabase import create_client
        url = st.secrets["supabase"]["url"]
        key = st.secrets["supabase"]["key"]
        return create_client(url, key)
    except Exception:
        return None


def save_invoice(sb, filename, carrier, invoice_total, parsed_total, df, dates=None):
    """Save parsed invoice and shipment rows to Supabase."""
    record = {
        "filename": filename,
        "carrier": carrier,
        "invoice_total": float(invoice_total) if invoice_total else None,
        "parsed_total": float(parsed_total),
    }
    if dates:
        if dates.get("invoice_date"):
            record["invoice_date"] = dates["invoice_date"]
        if dates.get("period_start"):
            record["period_start"] = dates["period_start"]
        if dates.get("period_end"):
            record["period_end"] = dates["period_end"]

    inv = sb.table("invoices").insert(record).execute()

    invoice_id = inv.data[0]["id"]

    # Insert shipment rows in batches of 500
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "invoice_id": invoice_id,
            "land": r["Land"],
            "belopp": float(r["Belopp (SEK)"]),
            "kolli": int(r["Kolli"]),
            "typ": r.get("Typ", ""),
            "detalj": r.get("Detalj", ""),
            "carrier": carrier,
            "filename": filename,
        })

    for i in range(0, len(rows), 500):
        sb.table("shipments").insert(rows[i:i+500]).execute()

    return invoice_id


def load_invoices(sb):
    """Load all invoices from Supabase."""
    result = sb.table("invoices").select("*").order("upload_date", desc=True).execute()
    return pd.DataFrame(result.data) if result.data else pd.DataFrame()


def load_shipments(sb, invoice_ids=None):
    """Load shipments, optionally filtered by invoice IDs."""
    query = sb.table("shipments").select("*")
    if invoice_ids:
        query = query.in_("invoice_id", invoice_ids)
    result = query.execute()
    if not result.data:
        return pd.DataFrame()
    df = pd.DataFrame(result.data)
    # Rename columns to match display format
    df = df.rename(columns={"land": "Land", "belopp": "Belopp (SEK)",
                            "kolli": "Kolli", "typ": "Typ", "detalj": "Detalj"})
    return df


def check_duplicate(sb, filename):
    """Check if a filename has already been uploaded."""
    result = sb.table("invoices").select("id").eq("filename", filename).execute()
    return len(result.data) > 0 if result.data else False


def delete_invoice(sb, invoice_id):
    """Delete an invoice and its shipments (cascade)."""
    sb.table("invoices").delete().eq("id", invoice_id).execute()


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED ANALYSIS VIEW
# ═══════════════════════════════════════════════════════════════════════════════

def show_analysis(df, invoice_total=None, n_files=1, overhead=None):
    """Render the full analysis view (metrics, type split, country table, charts)."""

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

    if n_files > 1:
        carriers = df["carrier"].unique() if "carrier" in df.columns else []
        if len(carriers) > 0:
            st.caption(f"📄 {n_files} fakturor ({', '.join(carriers)})")

    # Type split
    has_types = "Typ" in df.columns and df["Typ"].nunique() > 1
    if has_types:
        st.markdown("---")
        st.subheader("Uppdelning per typ")
        type_agg = (
            df.groupby("Typ")
            .agg(Kolli=("Kolli", "sum"), **{"Belopp (SEK)": ("Belopp (SEK)", "sum")})
            .sort_values("Belopp (SEK)", ascending=False).reset_index()
        )
        type_agg["Andel"] = (type_agg["Belopp (SEK)"] / total_amount * 100).round(1)
        type_cols = st.columns(len(type_agg))
        for i, row in type_agg.iterrows():
            with type_cols[i]:
                st.metric(row["Typ"], f"{row['Belopp (SEK)']:,.0f} SEK",
                          f"{row['Andel']:.1f}% — {int(row['Kolli']):,} kolli")

    # Country cross-tab
    st.markdown("---")
    st.subheader("Fördelning per land")

    if has_types:
        pivot_amount = df.pivot_table(index="Land", columns="Typ", values="Belopp (SEK)",
                                      aggfunc="sum", fill_value=0)
        pivot_kolli = df.pivot_table(index="Land", columns="Typ", values="Kolli",
                                     aggfunc="sum", fill_value=0)
        type_order = [t for t in ["Utgående", "Retur", "Övrigt"] if t in pivot_amount.columns]
        pivot_amount = pivot_amount.reindex(columns=type_order, fill_value=0)
        pivot_kolli = pivot_kolli.reindex(columns=type_order, fill_value=0)
        pivot_amount["Totalt"] = pivot_amount.sum(axis=1)
        pivot_kolli["Totalt kolli"] = pivot_kolli.sum(axis=1)

        country_table = pivot_amount.sort_values("Totalt", ascending=False).reset_index()
        # Align kolli by Land (not by independent sort!)
        kolli_by_land = pivot_kolli.reindex(pivot_amount.sort_values("Totalt", ascending=False).index)
        country_table["Kolli"] = kolli_by_land["Totalt kolli"].values

        for typ in type_order:
            kc = kolli_by_land[typ].values
            ac = country_table[typ].values
            country_table[f"Snitt {typ.lower()}"] = [
                round(a / k, 1) if k > 0 else 0.0 for a, k in zip(ac, kc)
            ]
        tk = kolli_by_land["Totalt kolli"].values
        country_table["Snitt totalt"] = [
            round(a / k, 1) if k > 0 else 0.0
            for a, k in zip(country_table["Totalt"].values, tk)
        ]
    else:
        country_table = (
            df.groupby("Land")
            .agg(Kolli=("Kolli", "sum"), **{"Totalt": ("Belopp (SEK)", "sum")})
            .sort_values("Totalt", ascending=False).reset_index()
        )
        country_table["Snitt totalt"] = (country_table["Totalt"] / country_table["Kolli"]).round(1)

    country_table["Andel"] = (country_table["Totalt"] / total_amount * 100).round(1)

    # Reconciliation row
    parsed_total = country_table["Totalt"].sum()
    if invoice_total and invoice_total > parsed_total + 1:
        gap = round(invoice_total - parsed_total, 2)
        gap_row = {"Land": "Ej allokerat", "Totalt": gap, "Kolli": 0,
                   "Andel": round(gap / invoice_total * 100, 1), "Snitt totalt": 0.0}
        if has_types:
            for typ in type_order:
                gap_row[typ] = 0.0
                gap_row[f"Snitt {typ.lower()}"] = 0.0
        country_table = pd.concat([country_table, pd.DataFrame([gap_row])], ignore_index=True)
        country_table["Andel"] = (country_table["Totalt"] / invoice_total * 100).round(1)

    # Charts
    chart_col1, chart_col2 = st.columns([3, 2])

    with chart_col1:
        if has_types:
            chart_data = country_table[country_table["Land"] != "Ej allokerat"].sort_values("Totalt", ascending=True)
            fig_bar = px.bar(
                chart_data.melt(id_vars=["Land"], value_vars=type_order, var_name="Typ", value_name="SEK"),
                x="SEK", y="Land", color="Typ", orientation="h",
                color_discrete_map={"Utgående": "#2171b5", "Retur": "#cb4b16", "Övrigt": "#999999"},
                text="SEK",
            )
            fig_bar.update_traces(texttemplate="%{text:,.0f}", textposition="inside", textfont_size=10)
            fig_bar.update_layout(
                title="Kostnad per land & typ (SEK)", xaxis_title="", yaxis_title="",
                barmode="stack", height=max(400, n_countries * 30 + 100),
                margin=dict(l=10, r=20, t=40, b=20), font=dict(family="DM Sans, sans-serif"),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
        else:
            fig_bar = px.bar(
                country_table[country_table["Land"] != "Ej allokerat"].sort_values("Totalt", ascending=True),
                x="Totalt", y="Land", orientation="h", text="Totalt", color="Totalt",
                color_continuous_scale=["#c6dbef", "#2171b5"],
            )
            fig_bar.update_traces(texttemplate="%{text:,.0f}", textposition="outside", textfont_size=11)
            fig_bar.update_layout(
                title="Kostnad per land (SEK)", xaxis_title="", yaxis_title="",
                coloraxis_showscale=False, height=max(400, n_countries * 30 + 100),
                margin=dict(l=10, r=80, t=40, b=20), font=dict(family="DM Sans, sans-serif"),
            )
        st.plotly_chart(fig_bar, use_container_width=True)

    with chart_col2:
        top_n = 8
        ct_clean = country_table[country_table["Land"] != "Ej allokerat"]
        if len(ct_clean) > top_n:
            top = ct_clean.head(top_n).copy()
            other_sum = ct_clean.iloc[top_n:]["Totalt"].sum()
            pie_data = pd.concat([top[["Land", "Totalt"]],
                                  pd.DataFrame([{"Land": "Övriga", "Totalt": other_sum}])],
                                 ignore_index=True)
        else:
            pie_data = ct_clean[["Land", "Totalt"]].copy()
        fig_pie = px.pie(pie_data, values="Totalt", names="Land",
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig_pie.update_traces(textposition="inside", textinfo="label+percent", textfont_size=11)
        fig_pie.update_layout(title="Andel per land", showlegend=False, height=450,
                              margin=dict(l=10, r=10, t=40, b=20),
                              font=dict(family="DM Sans, sans-serif"))
        st.plotly_chart(fig_pie, use_container_width=True)

    # Table
    st.markdown("---")
    st.subheader("Detaljerad tabell")
    display_df = country_table.copy()
    for col in display_df.columns:
        if col == "Land":
            continue
        if col == "Andel":
            display_df[col] = display_df[col].map("{:.1f}%".format)
        elif col == "Kolli":
            display_df[col] = display_df[col].astype(int)
        else:
            display_df[col] = display_df[col].map(
                lambda x: "{:,.0f}".format(x) if isinstance(x, (int, float)) else x
            )
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Overhead breakdown (if available)
    if overhead:
        st.markdown("---")
        st.subheader("Övriga kostnader (ej knutna till sändningar)")

        oh_df = pd.DataFrame(overhead)
        if not oh_df.empty:
            # Aggregate by category (in case of multiple invoices)
            oh_agg = oh_df.groupby("Kategori")["Belopp (SEK)"].sum().reset_index()
            oh_agg = oh_agg.sort_values("Belopp (SEK)", ascending=False)

            # Display as metric cards
            oh_total = oh_agg["Belopp (SEK)"].sum()
            oh_cols = st.columns(min(len(oh_agg), 4))
            for i, (_, row) in enumerate(oh_agg.iterrows()):
                with oh_cols[i % len(oh_cols)]:
                    pct = row["Belopp (SEK)"] / oh_total * 100 if oh_total > 0 else 0
                    st.metric(row["Kategori"], f"{row['Belopp (SEK)']:,.0f} SEK",
                              f"{pct:.0f}% av övriga kostnader")

            # Show unaccounted remainder
            if invoice_total:
                overhead_accounted = oh_total
                gap = invoice_total - parsed_total
                remainder = gap - overhead_accounted
                if abs(remainder) > 1:
                    st.caption(
                        f"Identifierade övriga kostnader: {overhead_accounted:,.0f} SEK av "
                        f"{gap:,.0f} SEK ej allokerat. "
                        f"Resterande {remainder:,.0f} SEK ospecificerat."
                    )
                else:
                    st.caption(
                        f"✅ Alla övriga kostnader identifierade: {overhead_accounted:,.0f} SEK."
                    )

    # Downloads
    st.markdown("---")
    dl1, dl2 = st.columns(2)
    with dl1:
        st.download_button("📥 Sammanfattning (CSV)",
                           country_table.to_csv(index=False, sep=";", decimal=","),
                           file_name="shipsplit_sammanfattning.csv", mime="text/csv")
    with dl2:
        st.download_button("📥 Alla rader (CSV)",
                           df.to_csv(index=False, sep=";", decimal=","),
                           file_name="shipsplit_detalj.csv", mime="text/csv")

    # Raw data expander
    with st.expander(f"Visa alla {n_lines} rader"):
        st.dataframe(df.sort_values("Belopp (SEK)", ascending=False),
                     use_container_width=True, hide_index=True)

    # Footer
    if invoice_total and invoice_total > parsed_total + 1:
        label = "Fakturornas" if n_files > 1 else "Fakturans"
        st.caption(
            f"ℹ️ {label} totalbelopp exkl. moms: **{invoice_total:,.0f} SEK**. "
            f"Allokerat: {parsed_total:,.0f} SEK ({parsed_total/invoice_total*100:.1f}%). "
            f"\"Ej allokerat\" ({invoice_total - parsed_total:,.0f} SEK) avser "
            f"korrigeringar och poster utan landskoppling."
        )
    elif invoice_total:
        st.caption(f"ℹ️ Totalbelopp exkl. moms: {invoice_total:,.0f} SEK — 100% allokerat.")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGES
# ═══════════════════════════════════════════════════════════════════════════════

def page_upload():
    st.title("📦 Ladda upp fakturor")
    st.markdown(
        "Ladda upp en eller flera fraktfakturor (PDF). Resultaten sparas automatiskt."
    )

    sb = init_supabase()

    uploaded_files = st.file_uploader(
        "Välj PDF-fakturor", type=["pdf"], accept_multiple_files=True,
        help="UPS, Bring, DHL Freight. Välj en eller flera.",
    )

    if not uploaded_files:
        return

    all_dfs = []
    total_invoice = 0.0
    has_any_total = False
    all_overhead = []
    msgs = []
    saved_count = 0

    with st.spinner(f"Analyserar {len(uploaded_files)} faktura(or)..."):
        for uf in uploaded_files:
            # Duplicate check
            if sb and check_duplicate(sb, uf.name):
                msgs.append(f"⏭️ **{uf.name}** — redan uppladdad, hoppad")
                continue

            with pdfplumber.open(uf) as pdf:
                sample = ""
                for p in pdf.pages[:3]:
                    t = p.extract_text()
                    if t:
                        sample += t + "\n"

            carrier = detect_carrier(sample)
            uf.seek(0)

            if carrier == "Unknown":
                msgs.append(f"⚠️ **{uf.name}** — okänt format")
                continue

            inv_total = None
            overhead = []
            if carrier == "UPS":
                df_part, inv_total, overhead = parse_ups_invoice(uf)
            elif carrier == "Bring":
                df_part, inv_total, overhead = parse_bring_invoice(uf)
            elif carrier == "DHL Freight":
                df_part, inv_total, overhead = parse_dhl_freight_invoice(uf)
            elif carrier in ("DHL Express", "DHL"):
                df_part, inv_total, overhead = parse_dhl_express_invoice(uf)
            else:
                df_part = pd.DataFrame()

            if df_part.empty:
                msgs.append(f"⚠️ **{uf.name}** — inga sändningar hittades")
                continue

            df_part["Faktura"] = uf.name
            df_part["Transportör"] = carrier
            all_dfs.append(df_part)

            if inv_total:
                total_invoice += inv_total
                has_any_total = True

            # Collect overhead charges
            all_overhead.extend(overhead)

            # Extract dates from invoice
            uf.seek(0)
            dates = extract_invoice_dates(uf)

            # Save to database
            if sb:
                try:
                    save_invoice(sb, uf.name, carrier, inv_total,
                                 df_part["Belopp (SEK)"].sum(), df_part, dates=dates)
                    saved_count += 1
                    msgs.append(
                        f"✅ **{uf.name}** — {carrier}, {len(df_part)} rader, "
                        f"{df_part['Belopp (SEK)'].sum():,.0f} SEK (sparad)"
                    )
                except Exception as e:
                    msgs.append(f"⚠️ **{uf.name}** — parsad men kunde inte sparas: {e}")
            else:
                msgs.append(
                    f"✅ **{uf.name}** — {carrier}, {len(df_part)} rader, "
                    f"{df_part['Belopp (SEK)'].sum():,.0f} SEK"
                )

    # Status messages
    if msgs:
        with st.expander(f"Filstatus ({len(uploaded_files)} fakturor)", expanded=len(uploaded_files) > 1):
            for msg in msgs:
                st.markdown(msg)

    if saved_count > 0 and sb:
        st.success(f"💾 {saved_count} faktura(or) sparade i databasen.")

    if not all_dfs:
        if msgs:
            st.info("Alla uppladdade fakturor var redan sparade eller kunde inte parsas.")
        return

    df = pd.concat(all_dfs, ignore_index=True)
    inv_total = total_invoice if has_any_total else None

    st.markdown("---")
    show_analysis(df, invoice_total=inv_total, n_files=len(all_dfs), overhead=all_overhead)


def page_history():
    st.title("📊 Historik & analys")

    sb = init_supabase()
    if not sb:
        st.error("Databasanslutning saknas. Kontrollera Supabase-inställningarna.")
        return

    invoices_df = load_invoices(sb)
    if invoices_df.empty:
        st.info("Inga fakturor uppladdade ännu. Gå till **Ladda upp** för att börja.")
        return

    # Parse dates — prefer period dates, fall back to upload_date
    invoices_df["upload_date"] = pd.to_datetime(invoices_df["upload_date"])
    invoices_df["period_start"] = pd.to_datetime(invoices_df["period_start"], errors="coerce")
    invoices_df["period_end"] = pd.to_datetime(invoices_df["period_end"], errors="coerce")

    # Display date: use period if available, else upload date
    invoices_df["display_date"] = pd.to_datetime(
        invoices_df["period_start"].fillna(invoices_df["upload_date"]),
        errors="coerce",
    )
    invoices_df["Period"] = invoices_df.apply(
        lambda r: (f"{r['period_start'].strftime('%Y-%m-%d')} — {r['period_end'].strftime('%Y-%m-%d')}"
                   if pd.notna(r["period_start"]) and pd.notna(r["period_end"])
                   else r["upload_date"].strftime("%Y-%m-%d")),
        axis=1,
    )

    # ── Filters ──────────────────────────────────────────────────────────
    st.subheader("Filter")
    f1, f2 = st.columns(2)

    with f1:
        carriers = sorted(invoices_df["carrier"].unique())
        sel_carriers = st.multiselect("Transportör", carriers, default=carriers)

    with f2:
        all_dates = invoices_df["display_date"].dropna()
        if not all_dates.empty:
            min_d = all_dates.min().date()
            max_d = all_dates.max().date()
        else:
            min_d = max_d = date.today()
        date_range = st.date_input("Period (orderperiod / fakturadatum)",
                                   value=(min_d, max_d), min_value=min_d, max_value=max_d)

    # Apply invoice-level filters
    filtered = invoices_df[invoices_df["carrier"].isin(sel_carriers)].copy()
    filtered["display_date"] = pd.to_datetime(filtered["display_date"], errors="coerce")
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start, end = date_range
        filtered = filtered[
            (filtered["display_date"].dt.date >= start) &
            (filtered["display_date"].dt.date <= end)
        ]

    if filtered.empty:
        st.warning("Inga fakturor matchar filtret.")
        return

    # ── Invoice list ─────────────────────────────────────────────────────
    st.markdown("---")
    st.subheader(f"Uppladdade fakturor ({len(filtered)} st)")

    display_inv = filtered[["Period", "filename", "carrier", "invoice_total", "parsed_total"]].copy()
    display_inv.columns = ["Period", "Filnamn", "Transportör", "Fakturabelopp", "Parsad summa"]
    display_inv["Fakturabelopp"] = display_inv["Fakturabelopp"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
    display_inv["Parsad summa"] = display_inv["Parsad summa"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "—")
    st.dataframe(display_inv, use_container_width=True, hide_index=True)

    # ── Load shipments ───────────────────────────────────────────────────
    invoice_ids = filtered["id"].tolist()
    total_inv = filtered["invoice_total"].sum() if filtered["invoice_total"].notna().any() else None

    with st.spinner("Laddar sändningsdata..."):
        df = load_shipments(sb, invoice_ids)

    if df.empty:
        st.warning("Inga sändningar hittade för de valda fakturorna.")
        return

    # ── Shipment-level filters ───────────────────────────────────────────
    st.markdown("---")
    sf1, sf2 = st.columns(2)

    with sf1:
        countries = sorted(df["Land"].unique())
        sel_countries = st.multiselect("Land", countries, default=countries,
                                       help="Filtrera analysen på specifika länder")

    with sf2:
        types = sorted(df["Typ"].dropna().unique()) if "Typ" in df.columns else []
        if types:
            sel_types = st.multiselect("Typ", types, default=types,
                                       help="Utgående, Retur, Övrigt")

    # Apply shipment-level filters
    df = df[df["Land"].isin(sel_countries)]
    if types and sel_types:
        df = df[df["Typ"].isin(sel_types)]

    if df.empty:
        st.warning("Inga sändningar matchar filtret.")
        return

    # Recalculate invoice total for filtered view
    if sel_countries != countries or (types and sel_types != types):
        # Filters changed — invoice total no longer meaningful
        total_inv = None

    st.markdown("---")
    show_analysis(df, invoice_total=total_inv, n_files=len(filtered))

    # ── Delete invoice ───────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("🗑️ Ta bort faktura"):
        del_options = {f"{r['Period']} — {r['filename']} ({r['carrier']})": r["id"]
                       for _, r in filtered.iterrows()}
        sel_del = st.selectbox("Välj faktura att ta bort", list(del_options.keys()))
        if st.button("Ta bort", type="secondary"):
            delete_invoice(sb, del_options[sel_del])
            st.success(f"Borttagen: {sel_del}")
            st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════════

if not check_password():
    st.stop()

page = st.sidebar.radio(
    "Navigation",
    ["📤 Ladda upp", "📊 Historik"],
    index=0,
)

if page == "📤 Ladda upp":
    page_upload()
elif page == "📊 Historik":
    page_history()
