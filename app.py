import streamlit as st
import requests
import pandas as pd
import pycountry
from urllib.parse import quote_plus

st.set_page_config(page_title="FDA Manufacturer Finder", layout="wide")

OPENFDA_BASE = "https://api.fda.gov"
REG_LISTING_ENDPOINT = f"{OPENFDA_BASE}/device/registrationlisting.json"
CLASS_ENDPOINT = f"{OPENFDA_BASE}/device/classification.json"

def country_to_iso2(name_or_code: str) -> str | None:
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
def lookup_product_codes_by_name(q: str, limit=50):
    # Search device classification by device name text to collect product codes
    # Example: search=device_name:pulse+oximeter
    query = f"search=device_name:{quote_plus(q)}&limit={limit}"
    url = f"{CLASS_ENDPOINT}?{query}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []
    data = r.json()
    results = data.get("results", [])
    codes = sorted({rec.get("product_code") for rec in results if rec.get("product_code")})
    return codes

def build_reglisting_query(iso2: str, product_codes: list[str], limit=1000, skip=0):
    search_parts = []
    if iso2:
        search_parts.append(f"registration.iso_country_code:{iso2}")
    # multiple product codes: (products.product_code:AAA+products.product_code:BBB)
    for pc in product_codes:
        if pc:
            search_parts.append(f"products.product_code:{pc.upper()}")
    search = "+".join(search_parts) if search_parts else ""
    params = f"limit={limit}&skip={skip}"
    return f"{REG_LISTING_ENDPOINT}?search={search}&{params}" if search else f"{REG_LISTING_ENDPOINT}?{params}"

@st.cache_data(show_spinner=True)
def fetch_reglisting(iso2: str, product_codes: list[str], max_records=2000):
    rows = []
    limit = 1000
    skip = 0
    fetched = 0
    while fetched < max_records:
        url = build_reglisting_query(iso2, product_codes, limit=limit, skip=skip)
        r = requests.get(url, timeout=60)
        if r.status_code != 200:
            break
        payload = r.json()
        results = payload.get("results", [])
        if not results:
            break
        rows.extend(results)
        n = len(results)
        fetched += n
        if n < limit:  # no more pages
            break
        skip += n
    return rows

st.title("FDA Manufacturer Finder")
st.caption("Filter FDA device establishments by **location** and **products** (openFDA).")

with st.sidebar:
    st.header("Filters")
    country_input = st.text_input("Country (name or ISO-2)", value="United States")
    iso2 = country_to_iso2(country_input) or ""
    mode = st.radio("Search by:", ["Product code(s)", "Device name"], horizontal=True)

    product_codes = []
    device_name = ""
    if mode == "Product code(s)":
        pcs = st.text_input("Product code(s), comma-separated", placeholder="e.g., DQD, MNO")
        product_codes = [p.strip().upper() for p in pcs.split(",") if p.strip()]
    else:
        device_name = st.text_input("Device name", placeholder="e.g., pulse oximeter")
        if device_name:
            with st.spinner("Finding product codes..."):
                product_codes = lookup_product_codes_by_name(device_name)

    st.write("Resolved product codes:", ", ".join(product_codes) if product_codes else "—")

    max_records = st.slider("Max records", 100, 5000, 2000, 100)
    go = st.button("Search", type="primary", disabled=not iso2 and not product_codes)

if go:
    with st.spinner("Querying openFDA…"):
        data = fetch_reglisting(iso2, product_codes, max_records=max_records)

    if not data:
        st.warning("No results. Try a different country or product selection.")
    else:
        # Normalize to table
        records = []
        for r in data:
            reg = r.get("registration", {}) or {}
            products = r.get("products", []) or []
            product_codes_join = ", ".join(sorted({p.get("product_code","") for p in products if p.get("product_code")}))
            est_types = ", ".join(sorted(set(r.get("establishment_type", [])))) if isinstance(r.get("establishment_type"), list) else r.get("establishment_type")
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
        st.success(f"Found {len(df):,} establishments")
        st.dataframe(df, use_container_width=True)
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "fda_mfrs.csv", "text/csv")
