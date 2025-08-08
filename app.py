import streamlit as st
import requests
import pandas as pd
import pycountry
from typing import Optional, List
from urllib.parse import quote_plus

st.set_page_config(page_title="FDA Manufacturer Finder", layout="wide")

OPENFDA_BASE = "https://api.fda.gov"
REG_LISTING_ENDPOINT = f"{OPENFDA_BASE}/device/registrationlisting.json"
CLASS_ENDPOINT = f"{OPENFDA_BASE}/device/classification.json"

# Optional: put an openFDA key in .streamlit/secrets.toml as:
# OPENFDA_API_KEY = "your_key_here"
OPENFDA_API_KEY = st.secrets.get("OPENFDA_API_KEY", None)
DEFAULT_HEADERS = {"X-Api-Key": OPENFDA_API_KEY} if OPENFDA_API_KEY else {}

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

def build_reglisting_search(iso2: str, product_codes: List[str], state_code: str = "") -> str:
    """
    Build the openFDA search string (we'll pass this in requests.get(params=...)).
      registration.iso_country_code:US
      registration.state_code.exact:CA  (only when iso2 == 'US'; note: NO quotes)
      products.product_code:DQD
    """
    parts = []
    if iso2:
        parts.append(f"registration.iso_country_code:{iso2}")
    if iso2 == "US" and state_code:
        parts.append(f"registration.state_code.exact:{state_code.upper()}")
    for pc in product_codes:
        if pc:
            parts.append(f"products.product_code:{pc.upper()}")
    return "+".join(parts)

@st.cache_data(show_spinner=True)
def fetch_reglisting(
    iso2: str,
    product_codes: List[str],
    state_code: str = "",
    max_records: int = 2000
):
    """Fetch paged results up to max_records."""
    rows = []
    limit = 1000
    skip = 0
    fetched = 0
    search = build_reglisting_search(iso2, product_codes, state_code)

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

# -------------------- UI --------------------

st.title("FDA Manufacturer Finder")
st.caption("Filter FDA device establishments by **location** and **products** (openFDA).")

with st.sidebar:
    st.header("Filters")
    country_input = st.text_input("Country (name or ISO-2)", value="United States")
    iso2 = country_to_iso2(country_input) or ""

    # --- State filter only for US ---
    state_code = ""
    if iso2 == "US":
        us_states = [
            "", "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN",
            "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV",
            "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
            "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]
        state_code = st.selectbox("Filter by state (optional)", options=us_states)

    mode = st.radio("Search by:", ["Product code(s)", "Device name"], horizontal=True)

    product_codes: List[str] = []
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

    max_records = st.slider("Max records", 100, 5000, 2000, 100)
    go = st.button("Search", type="primary", disabled=not iso2 and not product_codes)

# --- Query preview (always visible for debugging) ---
_preview_search = build_reglisting_search(iso2, product_codes, state_code)
_preview_params = {"search": _preview_search, "limit": 5, "skip": 0}
_preview = requests.Request("GET", REG_LISTING_ENDPOINT, params=_preview_params).prepare().url
st.caption("Query preview (first page, auto-encoded):")
st.code(_preview, language="text")

if go:
    with st.spinner("Querying openFDA…"):
        data = fetch_reglisting(iso2, product_codes, state_code=state_code, max_records=max_records)

    if not data:
        st.warning("No results. Try a different country/state or product selection.")
    else:
        records = []
        for r in data:
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

        st.success(f"Found {len(df):,} establishments")
        st.dataframe(df, use_container_width=True)
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "fda_mfrs.csv", "text/csv")

        with st.expander("Debug details"):
            st.write({
                "country_iso2": iso2,
                "state_code": state_code,
                "product_codes": product_codes,
                "api_key_present": bool(OPENFDA_API_KEY),
            })
else:
    st.info("Set your filters in the sidebar and click **Search**.")
