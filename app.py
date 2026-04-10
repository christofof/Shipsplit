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
import json
import gc
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
    """Streaming UPS parser — processes one page at a time and releases page
    cache immediately. Safe for very large invoices (1000+ pages, 10+ MB)."""

    # Pre-compile all regexes once
    mottagare_re = re.compile(r"Mottagare:\s+.+\s(\S+)\s*$")
    total_re = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)"
    )
    total_re_2 = re.compile(
        r"Total kostnad för sändning\s+(\S+)\s+SEK\s+([\d.,]+)\s+([\d.,]+)\s*$"
    )
    skickat_re = re.compile(r"Skickat från:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    avsandare_re = re.compile(r"Avsändare:.*\s([A-ZÅÄÖÜ][A-ZÅÄÖÜ\s]+)\s*$")
    totalkostnad_re = re.compile(
        r"Totalkostnad\s+SEK\s+[\d.,]+\s+(?:[\d.,]+\s+)?[\d.,]+\s+([\d.,]+)\s*$"
    )
    momspl_re = re.compile(r"Totalt momspliktigt\s+([\d.,]+)")
    icke_re = re.compile(r"Icke momspliktigt\s+([\d.,]+)")
    privat_re = re.compile(r"Totala justeringar för leveverans till privatadresser\s+SEK\s+([\d.,]+)\s+([\d.,]+)")
    sasong_re = re.compile(r"Totalkostnad för just\. av säsongsbas\. tilläggsavg\.\s+SEK\s+([\d.,]+)\s+([\d.,]+)")
    adress_re = re.compile(r"Total kostnad för adressändring\s+SEK\s+([\d.,]+)\s+([\d.,]+)")
    justeringar_re = re.compile(r"Totala justeringar\s+SEK\s+([\d.,]+)\s+([\d.,]+)")

    records = []
    # Outbound state (format 1)
    cur_out_country = None
    # Return state (format 2)
    cur_ret_country = None
    cur_section = None
    # Totals
    momspliktigt = 0.0
    icke_moms = 0.0
    privatadress_total = 0.0
    sasong_total = 0.0
    adress_total = 0.0
    justeringar_total = 0.0

    # Line-processing closure that updates all the state above.
    def _process_line(line):
        nonlocal cur_out_country, cur_ret_country, cur_section
        nonlocal momspliktigt, icke_moms
        nonlocal privatadress_total, sasong_total, adress_total, justeringar_total

        # ── Format 1: Outbound ──
        mm = mottagare_re.search(line)
        if mm:
            cur_out_country = normalize_country(mm.group(1))
        elif "Total kostnad för sändning" in line and cur_out_country:
            tm = total_re.search(line)
            if tm:
                records.append({
                    "Land": cur_out_country,
                    "Belopp (SEK)": parse_swedish_number(tm.group(4)),
                    "Kolli": 1, "Typ": "Utgående", "Detalj": tm.group(1),
                })
                cur_out_country = None
            else:
                tm2 = total_re_2.search(line)
                if tm2:
                    records.append({
                        "Land": cur_out_country,
                        "Belopp (SEK)": parse_swedish_number(tm2.group(3)),
                        "Kolli": 1, "Typ": "Utgående", "Detalj": tm2.group(1),
                    })
                    cur_out_country = None

        # ── Format 2: Returns ──
        s_lower = line.lower()
        if "UPS Returns" in line:
            cur_section = "Retur"
        elif "importsändningar" in s_lower:
            cur_section = "Retur"
        elif "icke levererbara returer" in s_lower:
            cur_section = "Retur"
        elif "upphämtningsbegäran" in s_lower:
            cur_section = "Övrigt"

        sm = skickat_re.search(line)
        if sm:
            cur_ret_country = normalize_country(sm.group(1))
        else:
            am = avsandare_re.search(line)
            if am:
                cur_ret_country = normalize_country(am.group(1))
            elif "Totalkostnad" in line and "SEK" in line and cur_ret_country:
                tm = totalkostnad_re.search(line)
                if tm:
                    records.append({
                        "Land": cur_ret_country,
                        "Belopp (SEK)": parse_swedish_number(tm.group(1)),
                        "Kolli": 1,
                        "Typ": cur_section or "Retur",
                        "Detalj": cur_section or "Retur",
                    })
                    cur_ret_country = None

        # ── Invoice total accumulators ──
        m = momspl_re.search(line)
        if m:
            momspliktigt = parse_swedish_number(m.group(1))
        m = icke_re.search(line)
        if m:
            icke_moms = parse_swedish_number(m.group(1))

        # ── Overhead accumulators ──
        m = privat_re.search(line)
        if m:
            privatadress_total += parse_swedish_number(m.group(2))
        m = sasong_re.search(line)
        if m:
            sasong_total += parse_swedish_number(m.group(2))
        m = adress_re.search(line)
        if m:
            adress_total += parse_swedish_number(m.group(2))
        m = justeringar_re.search(line)
        if m:
            justeringar_total += parse_swedish_number(m.group(2))

    # ── Chunked open/close to bound memory on very large invoices ──
    # pdfplumber leaks ~2.5 MB/page inside a single open(); closing between
    # chunks resets the internal caches so peak RAM stays ~flat.
    with pdfplumber.open(pdf_file) as pdf:
        n_pages = len(pdf.pages)
    CHUNK = 50 if n_pages and n_pages > 200 else (n_pages or 1)

    import gc
    for start in range(0, n_pages, CHUNK):
        end = min(start + CHUNK, n_pages)
        with pdfplumber.open(pdf_file) as pdf:
            for i in range(start, end):
                t = pdf.pages[i].extract_text()
                if not t:
                    continue
                for line in t.split("\n"):
                    _process_line(line)
        gc.collect()

    invoice_total = None
    if momspliktigt > 0 or icke_moms > 0:
        invoice_total = momspliktigt + icke_moms

    overhead = []

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

    # ── Fallback for manual/simple Bring invoices (e.g. reminders, storage) ──
    if not records:
        manual_re = re.compile(
            r"^\d+\s+\d+\s+\d+\s+(.+?)\s+(\d+)\s+St\s+([\d\s,]+)\s+(?:[\d.]+\s+)?(?:Export|Local VAT)\s+([\d\s,]+)$"
        )
        # Try to extract origin country from order line (POO/POA)
        country_m = re.search(r"POO:\s*\d+\s+([A-Z]{2})", full_text)
        fallback_country = normalize_country(country_m.group(1)) if country_m else "Okänt"

        for line in lines:
            m = manual_re.match(line.strip())
            if m:
                service = m.group(1).strip()
                qty = int(m.group(2))
                amount = float(m.group(4).replace(" ", "").replace(",", "."))
                is_return = "Return" in service
                records.append({
                    "Land": fallback_country, "Belopp (SEK)": amount,
                    "Kolli": qty, "Typ": "Retur" if is_return else "Utgående",
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

    # ── Map of all known DHL Freight service types ──
    DHL_SERVICE_MAP = {
        "SERVPOINT B2C": ("Servpoint B2C", "Utgående"),
        "SERVPOINT C2B": ("Servpoint C2B", "Retur"),
        "HOME DELIVERY": ("Home Delivery", "Utgående"),
        "HEMLEVERANS PAKET": ("Hemleverans Paket", "Utgående"),
        "DHL PAKET": ("Dhl Paket", "Utgående"),
    }

    lines = full_text.split("\n")
    spec_records = []
    current_service = current_typ = current_city = None

    for line in lines:
        s = line.strip()
        if re.match(r"^A\s+\d{3}\s", s):
            current_service = current_typ = None
            current_city = None
            s_upper = s.upper()
            for key, (name, typ) in DHL_SERVICE_MAP.items():
                if key in s_upper:
                    current_service = name
                    current_typ = typ
                    break
        elif re.match(r"^B\s+\d", s) and current_service:
            parts = s.split()
            if len(parts) >= 3:
                current_city = parts[2]
        elif re.match(r"^D\s", s) and current_service and "Tilläggs" not in s:
            m = re.search(r"(\d+)\s+(\d+)\s+(\d+,\d{2})\s*$", s)
            if m:
                spec_records.append({
                    "Land": "Sverige", "Belopp (SEK)": float(m.group(3).replace(",", ".")),
                    "Kolli": int(m.group(2)),
                    "Typ": current_typ,
                    "Detalj": f"{current_service} → {current_city or '?'}",
                })
                current_service = current_typ = current_city = None

    # ── Summary fallback — always try, use if better coverage ──
    summary_records = []
    service_pattern = "|".join(re.escape(k) for k in DHL_SERVICE_MAP)
    summary_re = re.compile(
        rf"({service_pattern})\s+(\d+)\s+([\d\s,]+?)(?:\s*\*)?$",
        re.MULTILINE,
    )
    for m in summary_re.finditer(full_text):
        key = m.group(1).upper().strip()
        name, typ = DHL_SERVICE_MAP.get(key, (m.group(1).title(), "Utgående"))
        count = int(m.group(2))
        amount = float(m.group(3).strip().replace(" ", "").replace(",", "."))
        summary_records.append({
            "Land": "Sverige", "Belopp (SEK)": amount, "Kolli": count,
            "Typ": typ,
            "Detalj": name,
        })

    # Use whichever method captured more revenue
    spec_total = sum(r["Belopp (SEK)"] for r in spec_records) if spec_records else 0
    summary_total = sum(r["Belopp (SEK)"] for r in summary_records) if summary_records else 0
    records = spec_records if spec_total >= summary_total else summary_records

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
        # Allow arbitrary content (e.g. <I>...</I> XML tags) between "Fakturadatum"
        # and the actual date on PDFs with interleaved metadata.
        month_alt = "|".join(months_sv.keys())
        m = re.search(
            rf"Fakturadatum[\s\S]{{0,200}}?(\d{{1,2}})\s+({month_alt})\s+(\d{{4}})",
            text,
            re.IGNORECASE,
        )
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


def save_invoice(sb, filename, carrier, invoice_total, parsed_total, df, dates=None, overhead=None):
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
    if overhead:
        record["overhead"] = json.dumps(overhead)

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
    """Load shipments, optionally filtered by invoice IDs.
    Paginates to overcome Supabase's 1000-row default limit."""
    all_rows = []

    def paginated_fetch(query):
        """Fetch all rows using range-based pagination."""
        rows = []
        page_size = 1000
        offset = 0
        while True:
            result = query.range(offset, offset + page_size - 1).execute()
            if result.data:
                rows.extend(result.data)
                if len(result.data) < page_size:
                    break  # Last page
                offset += page_size
            else:
                break
        return rows

    if invoice_ids:
        for inv_id in invoice_ids:
            query = sb.table("shipments").select("*").eq("invoice_id", inv_id)
            all_rows.extend(paginated_fetch(query))
    else:
        query = sb.table("shipments").select("*")
        all_rows = paginated_fetch(query)

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
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
# TIME-SERIES DASHBOARD (Phase 2)
# ═══════════════════════════════════════════════════════════════════════════════

def show_trends(df, invoices_df):
    """Render time-series trend charts.

    df: shipments DataFrame (Land, Belopp (SEK), Kolli, Typ, invoice_id, carrier)
    invoices_df: filtered invoices with period dates
    """

    # ── Build a period lookup: invoice_id → period label + sort key ──────
    inv_lookup = {}
    for _, row in invoices_df.iterrows():
        inv_id = row["id"]
        ps = row.get("period_start")
        pe = row.get("period_end")
        inv_date = row.get("invoice_date")
        carrier = row.get("carrier", "")

        NULL_STRS = ("None", "NaT", "nan", "NaN", "")

        # Best available date for this invoice
        if ps and str(ps) not in NULL_STRS:
            sort_key = str(ps)[:10]
        elif inv_date and str(inv_date) not in NULL_STRS:
            sort_key = str(inv_date)[:10]
        else:
            sort_key = "9999-99-99"

        # Period label
        if ps and pe and str(ps) not in NULL_STRS and str(pe) not in NULL_STRS:
            label = f"{str(ps)[:10]} — {str(pe)[:10]}"
        elif inv_date and str(inv_date) not in NULL_STRS:
            label = str(inv_date)[:10]
        else:
            label = "Okänt datum"

        inv_lookup[inv_id] = {"sort_key": sort_key, "label": label, "carrier": carrier}

    # Attach period info to shipments
    df = df.copy()
    df["period_sort"] = df["invoice_id"].map(lambda x: inv_lookup.get(x, {}).get("sort_key", "9999"))
    df["Period"] = df["invoice_id"].map(lambda x: inv_lookup.get(x, {}).get("label", "Okänt"))
    df["Transportör"] = df["invoice_id"].map(lambda x: inv_lookup.get(x, {}).get("carrier", ""))

    # Sort periods chronologically
    period_order = sorted(df[["period_sort", "Period"]].drop_duplicates().values.tolist())
    period_labels = [p[1] for p in period_order]

    if len(period_labels) < 2:
        st.info("Trendvyn kräver data från minst två perioder. Ladda upp fler fakturor.")
        return

    # ── Aggregate by period ──────────────────────────────────────────────
    period_agg = (
        df.groupby("Period")
        .agg(
            Total=("Belopp (SEK)", "sum"),
            Kolli=("Kolli", "sum"),
        )
        .reindex(period_labels)
        .reset_index()
    )
    period_agg["Snitt / kolli"] = (period_agg["Total"] / period_agg["Kolli"]).round(1)
    period_agg["Snitt / kolli"] = period_agg["Snitt / kolli"].fillna(0)

    # ── KPI cards: latest period vs previous ─────────────────────────────
    if len(period_agg) >= 2:
        curr = period_agg.iloc[-1]
        prev = period_agg.iloc[-2]

        def delta_str(curr_val, prev_val, unit="SEK", invert=False):
            if prev_val == 0:
                return None
            change = (curr_val - prev_val) / prev_val * 100
            sign = "+" if change > 0 else ""
            return f"{sign}{change:.1f}%"

        kc1, kc2, kc3 = st.columns(3)
        curr_label = str(curr["Period"])
        curr_short = curr_label[:10] if curr_label not in ("Okänt datum",) else curr_label
        kc1.metric(
            f"Totalkostnad ({curr_short})",
            f"{curr['Total']:,.0f} SEK",
            delta_str(curr["Total"], prev["Total"]),
            delta_color="inverse",
        )
        kc2.metric(
            "Kolli",
            f"{int(curr['Kolli']):,}",
            delta_str(curr["Kolli"], prev["Kolli"]),
        )
        kc3.metric(
            "Snitt / kolli",
            f"{curr['Snitt / kolli']:,.1f} SEK",
            delta_str(curr["Snitt / kolli"], prev["Snitt / kolli"]),
            delta_color="inverse",
        )

    st.markdown("---")

    # ── Chart 1: Total cost per period, stacked by carrier ───────────────
    carrier_period = (
        df.groupby(["Period", "Transportör"])
        .agg(Total=("Belopp (SEK)", "sum"))
        .reset_index()
    )
    carrier_period["Period"] = pd.Categorical(carrier_period["Period"], categories=period_labels, ordered=True)
    carrier_period = carrier_period.sort_values("Period")

    fig_cost = px.bar(
        carrier_period, x="Period", y="Total", color="Transportör",
        color_discrete_map={"UPS": "#2171b5", "Bring": "#43a047", "DHL Freight": "#fdd835"},
        text="Total", barmode="stack",
    )
    fig_cost.update_traces(texttemplate="%{text:,.0f}", textposition="inside", textfont_size=10)
    fig_cost.update_layout(
        title="Total fraktkostnad per period",
        xaxis_title="", yaxis_title="SEK", height=400,
        margin=dict(l=10, r=20, t=40, b=20),
        font=dict(family="DM Sans, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_cost, use_container_width=True)

    # ── Chart 2: Cost per kolli over time, by carrier ────────────────────
    carrier_avg = (
        df[df["Kolli"] > 0]
        .groupby(["Period", "Transportör"])
        .agg(Total=("Belopp (SEK)", "sum"), Kolli=("Kolli", "sum"))
        .reset_index()
    )
    carrier_avg["Snitt / kolli"] = (carrier_avg["Total"] / carrier_avg["Kolli"]).round(1)
    carrier_avg["Period"] = pd.Categorical(carrier_avg["Period"], categories=period_labels, ordered=True)
    carrier_avg = carrier_avg.sort_values("Period")

    fig_avg = px.line(
        carrier_avg, x="Period", y="Snitt / kolli", color="Transportör",
        color_discrete_map={"UPS": "#2171b5", "Bring": "#43a047", "DHL Freight": "#fdd835"},
        markers=True,
    )
    fig_avg.update_layout(
        title="Kostnad per kolli — trend per transportör",
        xaxis_title="", yaxis_title="SEK / kolli", height=350,
        margin=dict(l=10, r=20, t=40, b=20),
        font=dict(family="DM Sans, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_avg, use_container_width=True)

    # ── Chart 3: Outbound vs Return over time ────────────────────────────
    type_period = (
        df.groupby(["Period", "Typ"])
        .agg(Total=("Belopp (SEK)", "sum"))
        .reset_index()
    )
    type_period["Period"] = pd.Categorical(type_period["Period"], categories=period_labels, ordered=True)
    type_period = type_period.sort_values("Period")

    fig_type = px.bar(
        type_period, x="Period", y="Total", color="Typ",
        color_discrete_map={"Utgående": "#2171b5", "Retur": "#cb4b16", "Övrigt": "#999999"},
        barmode="stack", text="Total",
    )
    fig_type.update_traces(texttemplate="%{text:,.0f}", textposition="inside", textfont_size=10)
    fig_type.update_layout(
        title="Utgående vs retur — trend",
        xaxis_title="", yaxis_title="SEK", height=350,
        margin=dict(l=10, r=20, t=40, b=20),
        font=dict(family="DM Sans, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_type, use_container_width=True)

    # ── Chart 4: Top countries over time ─────────────────────────────────
    country_totals = df.groupby("Land")["Belopp (SEK)"].sum().sort_values(ascending=False)
    top_countries = country_totals.head(6).index.tolist()

    country_trend = (
        df[df["Land"].isin(top_countries)]
        .groupby(["Period", "Land"])
        .agg(Total=("Belopp (SEK)", "sum"))
        .reset_index()
    )
    country_trend["Period"] = pd.Categorical(country_trend["Period"], categories=period_labels, ordered=True)
    country_trend = country_trend.sort_values("Period")

    fig_country = px.line(
        country_trend, x="Period", y="Total", color="Land",
        markers=True, color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_country.update_layout(
        title="Kostnadstrend — topp 6 länder",
        xaxis_title="", yaxis_title="SEK", height=350,
        margin=dict(l=10, r=20, t=40, b=20),
        font=dict(family="DM Sans, sans-serif"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig_country, use_container_width=True)

    # ── Period comparison table ───────────────────────────────────────────
    st.markdown("---")
    st.subheader("Periodöversikt")
    period_detail = (
        df.groupby("Period")
        .agg(
            Transportörer=("Transportör", lambda x: ", ".join(sorted(x.unique()))),
            Total=("Belopp (SEK)", "sum"),
            Kolli=("Kolli", "sum"),
            Länder=("Land", "nunique"),
            Rader=("Land", "count"),
        )
        .reindex(period_labels)
        .reset_index()
    )
    period_detail["Snitt / kolli"] = (period_detail["Total"] / period_detail["Kolli"]).round(1)
    period_detail["Snitt / kolli"] = period_detail["Snitt / kolli"].fillna(0)

    display_pd = period_detail.copy()
    display_pd["Total"] = display_pd["Total"].map("{:,.0f}".format)
    display_pd["Kolli"] = display_pd["Kolli"].astype(int).map("{:,}".format)
    display_pd["Snitt / kolli"] = display_pd["Snitt / kolli"].map("{:,.1f}".format)
    display_pd.columns = ["Period", "Transportörer", "Totalt (SEK)", "Kolli", "Länder", "Rader", "Snitt/kolli (SEK)"]
    st.dataframe(display_pd, use_container_width=True, hide_index=True)


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

    progress = st.progress(0, text="Analyserar fakturor...")
    for file_idx, uf in enumerate(uploaded_files):
        progress.progress((file_idx) / len(uploaded_files),
                          text=f"Analyserar {uf.name}... ({file_idx+1}/{len(uploaded_files)})")

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
        del sample
        uf.seek(0)

        if carrier == "Unknown":
            msgs.append(f"⚠️ **{uf.name}** — okänt format")
            continue

        inv_total = None
        overhead = []
        try:
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
        except MemoryError:
            msgs.append(f"❌ **{uf.name}** — för stor fil, minnet räcker inte")
            gc.collect()
            continue

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
                             df_part["Belopp (SEK)"].sum(), df_part, dates=dates,
                             overhead=overhead)
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

        # Free memory before next file
        gc.collect()

    progress.empty()

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

    # Parse dates safely as strings for display, timestamps for sorting
    invoices_df["upload_date"] = pd.to_datetime(invoices_df["upload_date"], errors="coerce")

    def safe_date_str(row):
        """Build a display string for the period."""
        ps = row.get("period_start")
        pe = row.get("period_end")
        if ps and pe and str(ps) != "None" and str(pe) != "None":
            return f"{str(ps)[:10]} — {str(pe)[:10]}"
        ud = row.get("upload_date")
        if pd.notna(ud):
            return str(ud)[:10]
        return "Okänt datum"

    invoices_df["Period"] = invoices_df.apply(safe_date_str, axis=1)

    # Sort date for filtering (use period_start string → date, fallback to upload_date)
    def safe_sort_date(row):
        ps = row.get("period_start")
        if ps and str(ps) not in ("None", "NaT", ""):
            try:
                return pd.Timestamp(str(ps)[:10])
            except Exception:
                pass
        return row.get("upload_date", pd.NaT)

    invoices_df["sort_date"] = invoices_df.apply(safe_sort_date, axis=1)
    invoices_df["sort_date"] = pd.to_datetime(invoices_df["sort_date"], errors="coerce")

    # ── Quick filters ────────────────────────────────────────────────────
    st.subheader("Filter")

    today = date.today()
    period_options = {
        "Alla": None,
        "Denna månad": (today.replace(day=1), today),
        "Förra månaden": (
            (today.replace(day=1) - timedelta(days=1)).replace(day=1),
            today.replace(day=1) - timedelta(days=1),
        ),
        "Senaste 3 mån": (today - timedelta(days=90), today),
        "Senaste 6 mån": (today - timedelta(days=180), today),
        "Senaste 12 mån": (today - timedelta(days=365), today),
    }

    sel_period = st.radio("Snabbval period", list(period_options.keys()),
                          horizontal=True, index=0)

    carriers = sorted(invoices_df["carrier"].unique())
    sel_carriers = st.multiselect("Transportör", carriers, default=carriers)

    # Apply carrier filter
    filtered = invoices_df[invoices_df["carrier"].isin(sel_carriers)].copy()

    # Apply period filter
    if period_options[sel_period] is not None:
        p_start, p_end = period_options[sel_period]
        p_start_ts = pd.Timestamp(p_start)
        p_end_ts = pd.Timestamp(p_end) + pd.Timedelta(days=1, seconds=-1)
        mask = filtered["sort_date"].notna() & (filtered["sort_date"] >= p_start_ts) & (filtered["sort_date"] <= p_end_ts)
        filtered = filtered[mask]

    # ── Invoice picker (primary selection) ────────────────────────────────
    st.markdown("---")
    st.subheader("Välj fakturor att analysera")

    if filtered.empty:
        st.warning("Inga fakturor matchar filtret.")
        return

    # Build label for each invoice
    def inv_label(row):
        amt = f"{row['parsed_total']:,.0f} SEK" if pd.notna(row.get("parsed_total")) else "—"
        return f"{row['Period']}  ·  {row['carrier']}  ·  {row['filename']}  ·  {amt}"

    filtered["label"] = filtered.apply(inv_label, axis=1)
    label_to_id = dict(zip(filtered["label"], filtered["id"]))

    sel_labels = st.multiselect(
        "Fakturor i databasen",
        list(label_to_id.keys()),
        default=list(label_to_id.keys()),
        help="Avmarkera fakturor du vill exkludera från analysen.",
    )

    sel_ids = [label_to_id[lbl] for lbl in sel_labels]
    filtered = filtered[filtered["id"].isin(sel_ids)]

    if filtered.empty:
        st.warning("Inga fakturor valda.")
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

    # ── Shared shipment-level filters (apply to both tabs) ──────────────
    st.markdown("---")
    st.subheader("Sändningsfilter")
    sf1, sf2 = st.columns(2)

    with sf1:
        countries = sorted(df["Land"].unique())
        sel_countries = st.multiselect(
            "Land", countries, default=countries,
            help="Filtrerar både Trender och Översikt",
        )

    with sf2:
        types = sorted(df["Typ"].dropna().unique()) if "Typ" in df.columns else []
        if types:
            sel_types = st.multiselect(
                "Typ", types, default=types,
                help="Utgående, Retur, Övrigt — filtrerar båda vyerna",
            )
        else:
            sel_types = []

    # Apply shipment-level filters once
    df_filtered = df[df["Land"].isin(sel_countries)].copy()
    if types and sel_types:
        df_filtered = df_filtered[df_filtered["Typ"].isin(sel_types)]

    filters_active = (sel_countries != countries) or (bool(types) and sel_types != types)

    if df_filtered.empty:
        st.warning("Inga sändningar matchar filtret.")
        return

    # ── Tabs: Trender + Översikt ─────────────────────────────────────────
    tab_trends, tab_overview = st.tabs(["📈 Trender", "📊 Översikt"])

    with tab_trends:
        show_trends(df_filtered, filtered)

    with tab_overview:
        # Recalculate invoice total for filtered view
        total_inv_view = None if filters_active else total_inv

        # Load overhead from stored invoices (skip if shipment filters active)
        all_overhead = []
        if not filters_active:
            for _, inv_row in filtered.iterrows():
                oh_json = inv_row.get("overhead")
                if oh_json and isinstance(oh_json, str):
                    try:
                        all_overhead.extend(json.loads(oh_json))
                    except (json.JSONDecodeError, TypeError):
                        pass

        show_analysis(df_filtered, invoice_total=total_inv_view, n_files=len(filtered),
                      overhead=all_overhead if all_overhead else None)

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
