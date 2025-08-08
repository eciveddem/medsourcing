import streamlit as st
import requests
import pandas as pd
import pycountry
from typing import Optional, List, Dict
from urllib.parse import quote_plus

st.set_page_config(page_title="FDA Manufacturer Finder", layout="wide")

OPENFDA_BASE = "https://api.fda.gov"
REG_LISTING_ENDPOINT = f"{OPENFDA_BASE}/device/registrationlisting.json"
CLASS_ENDPOINT = f"{OPENFDA_BASE}/device/classification.json"
K510_ENDPOINT = f"{OPENFDA_BASE}/device/510k.json"
MAUDE_ENDPOINT = f"{OPENFDA_BASE}/device/event.json"

# Optional: put an openFDA key in .streamlit/secrets.toml as:
# OPENFDA_API_KEY = "your_key_here"
OPENFDA_API_KEY = st.secrets.get("OPENFDA_API_KEY", None)
DEFAULT_HEADERS = {"X-Api-Key": OPENFDA_API_KEY} if OPENFDA_API_KEY else {}

# -------------------- helpers --------------------

def country_to_iso2(name_or_code: str) -> Optional[str]:
    """Accepts 'United States' or 'US' and returns ISO-2 like 'US'."""
    if not name_or_code:
        return None
    s = name_or_code.strip()
    if len(s) == 2:
        return s.upper()
    try:
        return pycountry.countries.lookup(s).alpha_2
    except Exception:
        return None

@st.cache_data(show_spinner=False)
def lookup_product_codes_by_name(q: str, limit=50) -> List[str]:
    """Search device classification by device name, return unique product codes."""
    query = f"search=device_name:{quote_plus(q)}&limit={limit}"
    url = f"{CLASS_ENDPOINT}?{query}"
    r = requests.get(url, timeout=30, headers=DEFAULT_HEADERS)
    if r.status_code != 200:
        return []
    data = r.json()
    results = data.get("results", [])
    codes = sorted({rec.get("product_code") for rec in results if rec.get("product_code")})
    return codes

def build_reglisting_search(iso2: str, product_codes: List[str]) -> str:
    """
    Build the openFDA search string for Registrations & Listings.
      registration.iso_country_code:US
      products.product_code:DQD
    """
    parts = []
    if iso2:
        parts.append(f"registration.iso_country_code:{iso2}")
    for pc in product_codes:
        if pc:
            parts.append(f"products.product_code:{pc.upper()}")
    return "+".join(parts)

@st.cache_data(show_spinner=True)
def fetch_reglisting(
    iso2: str,
    product_codes: List[str],
    max_records: int = 2000
):
    """Fetch registrations/listings (paged) up to max_records."""
    rows = []
    limit = 1000
    skip = 0
    fetched = 0
    search = build_reglisting_search(iso2, product_codes)

    while fetched < max_records:
        params = {"search": search, "limit": limit, "skip": skip}
        r = requests.get(REG_LISTING_ENDPOINT, params=params, timeout=60, headers=DEFAULT_HEADERS)
        if r.status_code != 200:
            break
        payload = r.json()
        results = payload.get("results", [])
        if not results:
            break
        rows.extend(results)
        n = len(results)
        fetched += n
        if n < limit:
            break
        skip += n
    return rows

def normalize_reglisting_rows(rows: List[Dict]) -> pd.DataFrame:
    records = []
    for r in rows:
        reg = r.get("registration", {}) or {}
        products = r.get("products", []) or []
        product_codes_join = ", ".join(sorted({p.get("product_code","") for p in products if p.get("product_code")}))
        est_types = r.get("establishment_type")
        if isinstance(est_types, list):
            est_types = ", ".join(sorted(set(est_types)))
        records.append({
            "FEI": reg.get("fei_number"),
            "Firm Name": reg.get("name"),
            "City": reg.get("city"),
            "State/Prov": reg.get("state_code") or reg.get("state_province"),
            "Country": reg.get("iso_country_code"),
            "Establishment Types": est_types,
            "Product Codes": product_codes_join,
        })
    df = pd.DataFrame.from_records(records).drop_duplicates()
    # helpful dropdown label
    df["Firm Label"] = df.apply(lambda x: f'{x["Firm Name"]} — {x["City"] or ""} {x["State/Prov"] or ""} ({x["Country"]})', axis=1)
    return df

# -------------------- 510(k) lookup --------------------

def build_510k_search(applicant: Optional[str], product_codes: List[str]) -> List[str]:
    """
    Construct one or more 510(k) search strings.
    Strategy:
      1) If we have a firm name, try applicant:<firm name> AND product_code.
      2) Fallback: product_code only (will show others too, but useful).
    Returns a de-duplicated list of queries to run.
    """
    queries = set()
    pcs = [pc for pc in product_codes if pc]
    if applicant:
        # one query per product code with applicant filter
        for pc in pcs:
            queries.add(f'applicant:"{applicant}" + product_code:{pc}')
    # Fallback if nothing or to widen results
    for pc in pcs:
        queries.add(f"product_code:{pc}")
    return list(queries)

@st.cache_data(show_spinner=True)
def fetch_510k(applicant: Optional[str], product_codes: List[str], max_per_query: int = 200) -> pd.DataFrame:
    """Fetch 510(k) clearances filtered by applicant & product code where possible."""
    queries = build_510k_search(applicant, product_codes)
    frames = []
    for q in queries:
        params = {"search": q, "limit": max_per_query}
        r = requests.get(K510_ENDPOINT, params=params, timeout=60, headers=DEFAULT_HEADERS)
        if r.status_code != 200:
            continue
        results = (r.json() or {}).get("results", [])
        if not results:
            continue
        for rec in results:
            pass
        df = pd.json_normalize(results)
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["k_number","applicant","product_code","decision_date","device_name"])
    df_all = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["k_number"], keep="first")
    # Keep common columns, rename if nested
    cols = {}
    for c in df_all.columns:
        lc = c.lower()
        if lc.endswith("k_number"): cols[c] = "k_number"
        elif lc.endswith("applicant"): cols[c] = "applicant"
        elif lc.endswith("product_code"): cols[c] = "product_code"
        elif lc.endswith("decision_date"): cols[c] = "decision_date"
        elif lc.endswith("device_name"): cols[c] = "device_name"
    df_all = df_all.rename(columns=cols)
    keep = ["k_number","applicant","product_code","decision_date","device_name"]
    for k in keep:
        if k not in df_all.columns:
            df_all[k] = None
    return df_all[keep].sort_values(by="decision_date", ascending=False, na_position="last")

# -------------------- MAUDE (device event) --------------------

def build_maude_queries(firm_name: str, product_codes: List[str]) -> List[Dict]:
    """
    We try two sensible avenues (run until we get results):
      A) manufacturer_name:"Firm Name"
      B) device.manufacturer_d_name:"Firm Name"
    We also include product_code filters to narrow to the firm's listed products.
    """
    pcs = [pc for pc in product_codes if pc]
    if not pcs:
        base_filters = [f'manufacturer_name:"{firm_name}"', f'device.manufacturer_d_name:"{firm_name}"']
    else:
        pc_filters = [f"device.product_code:{pc}" for pc in pcs]
        base_filters = [
            f'manufacturer_name:"{firm_name}"+(' + "+".join(pc_filters) + ")",
            f'device.manufacturer_d_name:"{firm_name}"+(' + "+".join(pc_filters) + ")",
        ]
    # Return each as a ready-to-use search string
    return base_filters

@st.cache_data(show_spinner=True)
def fetch_maude_year_counts(firm_name: str, product_codes: List[str], max_records: int = 5000) -> pd.DataFrame:
    """
    Pull MAUDE events and return counts by year (from date_received).
    Caps total records to avoid huge pulls.
    """
    queries = build_maude_queries(firm_name, product_codes)
    limit = 1000
    year_counts = {}

    for q in queries:
        fetched = 0
        skip = 0
        while fetched < max_records:
            params = {"search": q, "limit": limit, "skip": skip}
            r = requests.get(MAUDE_ENDPOINT, params=params, timeout=60, headers=DEFAULT_HEADERS)
            if r.status_code != 200:
                break
            payload = r.json() or {}
            results = payload.get("results", [])
            if not results:
                break
            for rec in results:
                # Try date_received first, else event_date if present
                date_str = rec.get("date_received") or rec.get("event_date")
                if not date_str:
                    continue
                # Expect YYYYMMDD
                year = str(date_str)[:4]
                if year.isdigit():
                    year_counts[year] = year_counts.get(year, 0) + 1
            n = len(results)
            fetched += n
            if n < limit:
                break
            skip += n
        # If we got some counts with the first strategy, we can stop early
        if year_counts:
            break

    if not year_counts:
        return pd.DataFrame(columns=["year","count"])

    df = pd.DataFrame(sorted(year_counts.items()), columns=["year","count"])
    df["year"] = df["year"].astype(int)
    df = df.sort_values("year")
    return df

# -------------------- UI --------------------

st.title("FDA Manufacturer Finder")
st.caption("Filter FDA device establishments by **country** and **products**, then explore **510(k)** and **MAUDE** for a selected firm (openFDA).")

with st.sidebar:
    st.header("Filters")
    country_input = st.text_input("Country (name or ISO-2)", value="United States")
    iso2 = country_to_iso2(country_input) or ""

    mode = st.radio("Search by:", ["Product code(s)", "Device name"], horizontal=True)

    product_codes: List[str] = []
    device_name = ""
    if mode == "Product code(s)":
        pcs = st.text_input("Product code(s), comma-separated", placeholder="e.g., DQD, FMF")
        product_codes = [p.strip().upper() for p in pcs.split(",") if p.strip()]
    else:
        device_name = st.text_input("Device name", placeholder="e.g., pulse oximeter")
        if device_name:
            with st.spinner("Finding product codes..."):
                product_codes = lookup_product_codes_by_name(device_name)

    resolved = ", ".join(product_codes) if product_codes else "—"
    st.write("Resolved product codes:", resolved)

    max_records = st.slider("Max registrations to pull", 100, 5000, 2000, 100)
    go = st.button("Search", type="primary", disabled=not iso2 and not product_codes)

# --- Query preview (first page, auto-encoded) ---
_preview_search = build_reglisting_search(iso2, product_codes)
_preview_params = {"search": _preview_search, "limit": 5, "skip": 0}
_preview = requests.Request("GET", REG_LISTING_ENDPOINT, params=_preview_params).prepare().url
st.caption("Registration query preview:")
st.code(_preview, language="text")

# -------------------- Results & drill-in --------------------

if go:
    with st.spinner("Querying openFDA Registrations…"):
        data = fetch_reglisting(iso2, product_codes, max_records=max_records)

    if not data:
        st.warning("No results. Try a different country or product selection.")
    else:
        df_regs = normalize_reglisting_rows(data)
        st.success(f"Found {len(df_regs):,} establishments")
        st.dataframe(df_regs.drop(columns=["Firm Label"]), use_container_width=True)

        # Select a manufacturer to drill down
        st.subheader("Select a manufacturer")
        options = df_regs["Firm Label"].tolist()
        selected_label = st.selectbox("Manufacturer", options=options, index=0 if options else None)

        if selected_label:
            selected_row = df_regs[df_regs["Firm Label"] == selected_label].iloc[0]
            firm_name = selected_row["Firm Name"]
            firm_fei = selected_row["FEI"]
            firm_pcs = [pc.strip().upper() for pc in (selected_row["Product Codes"] or "").split(",") if pc.strip()]

            st.info(f"Selected: **{firm_name}**  |  FEI: {firm_fei or '—'}  |  Product Codes: {', '.join(firm_pcs) or '—'}")

            tab_overview, tab_510k, tab_maude = st.tabs(["Overview", "510(k)", "MAUDE"])

            with tab_overview:
                st.write("**Registration details**")
                st.json({
                    "Firm Name": firm_name,
                    "FEI": firm_fei,
                    "City": selected_row["City"],
                    "State/Prov": selected_row["State/Prov"],
                    "Country": selected_row["Country"],
                    "Establishment Types": selected_row["Establishment Types"],
                    "Product Codes": firm_pcs,
                })

            with tab_510k:
                st.write("**510(k) clearances** (matching firm/product code where possible)")
                with st.spinner("Searching 510(k) by applicant and product code…"):
                    df_510k = fetch_510k(applicant=firm_name, product_codes=firm_pcs, max_per_query=200)
                if df_510k.empty:
                    st.warning("No 510(k) records found for this selection (try different product codes or firm).")
                else:
                    st.dataframe(df_510k, use_container_width=True)
                    st.download_button(
                        "Download 510(k) CSV",
                        df_510k.to_csv(index=False).encode("utf-8"),
                        "510k_results.csv",
                        "text/csv"
                    )

            with tab_maude:
                st.write("**MAUDE device events** — yearly count (by date received)")
                with st.spinner("Fetching MAUDE events and aggregating by year…"):
                    df_years = fetch_maude_year_counts(firm_name=firm_name, product_codes=firm_pcs, max_records=5000)
                if df_years.empty:
                    st.warning("No MAUDE events found for this firm (with current filters).")
                else:
                    st.bar_chart(df_years.set_index("year")["count"])
                    st.dataframe(df_years, use_container_width=True)
else:
    st.info("Set your filters in the sidebar and click **Search**.")
