import streamlit as st
import requests
import pandas as pd
import pycountry
from urllib.parse import quote_plus

st.set_page_config(page_title="FDA Manufacturer Finder", layout="wide")

OPENFDA_BASE = "https://api.fda.gov"
REG_LISTING_ENDPOINT = f"{OPENFDA_BASE}/device/registrationlisting.json"
CLASS_ENDPOINT = f"{OPENFDA_BASE}/device/classification.json"
MAUDE_ENDPOINT = f"{OPENFDA_BASE}/device/event.json"

# -------------------- helpers --------------------

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
    # multiple product codes => AND; to keep recall broad, we just AND them if provided
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

def normalize_reglisting_rows(rows: list[dict]) -> pd.DataFrame:
    records = []
    for r in rows:
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
    # Helpful label for selection (keeps it readable when names repeat)
    df["Firm Label"] = df.apply(lambda x: f'{x["Firm Name"]} — {x["City"] or ""} {x["State/Prov"] or ""} ({x["Country"]})', axis=1)
    return df

# -------------------- MAUDE lookups (last 18 months) --------------------

def last_18_month_window() -> tuple[pd.Timestamp, pd.Timestamp, pd.PeriodIndex]:
    """Return inclusive month window and a PeriodIndex for the last 18 months ending this month."""
    today = pd.Timestamp.today().normalize()
    # End month is current month; 18 months including current => periods=18
    months = pd.period_range(end=today.to_period("M"), periods=18, freq="M")
    start_date = months[0].to_timestamp(how="start")
    end_date = months[-1].to_timestamp(how="end")
    return start_date, end_date, months

def build_maude_query(firm_name: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> list[str]:
    """
    Build two manufacturer-name queries with date_received window:
      A) manufacturer_name:"Firm Name"
      B) device.manufacturer_d_name:"Firm Name"
    We do NOT AND product codes here (it would over-restrict).
    """
    date_clause = f'date_received:[{start_date:%Y%m%d}+TO+{end_date:%Y%m%d}]'
    base1 = f'manufacturer_name:"{firm_name}"'
    base2 = f'device.manufacturer_d_name:"{firm_name}"'
    return [f"{base1}+{date_clause}", f"{base2}+{date_clause}"]

@st.cache_data(show_spinner=True)
def fetch_maude_events_18m(firm_name: str, max_records: int = 5000) -> pd.DataFrame:
    """Fetch MAUDE events for the firm over the last 18 months; returns a normalized DataFrame."""
    start_date, end_date, _ = last_18_month_window()
    queries = build_maude_query(firm_name, start_date, end_date)

    frames = []
    limit = 1000
    for q in queries:
        fetched = 0
        skip = 0
        while fetched < max_records:
            params = {"search": q, "limit": limit, "skip": skip}
            r = requests.get(MAUDE_ENDPOINT, params=params, timeout=60)
            if r.status_code != 200:
                break
            payload = r.json() or {}
            results = payload.get("results", [])
            if not results:
                break
            df = pd.json_normalize(results)
            frames.append(df)
            n = len(results)
            fetched += n
            if n < limit:
                break
            skip += n

    if not frames:
        return pd.DataFrame(columns=[
            "date_received", "event_type", "device.product_code", "device.brand_name", "manufacturer_name"
        ])

    df_all = pd.concat(frames, ignore_index=True)
    # Keep a few common/useful columns if present
    keep_map = {
        "date_received": "date_received",
        "event_type": "event_type",
        "device.product_code": "device.product_code",
        "device.brand_name": "device.brand_name",
        "manufacturer_name": "manufacturer_name",
        "device.manufacturer_d_name": "device.manufacturer_d_name",
        "event_location": "event_location",
        "adverse_event_flag": "adverse_event_flag",
        "reporter_occupation_code": "reporter_occupation_code",
    }
    cols = [c for c in df_all.columns if c in keep_map]
    df_all = df_all[cols].copy()
    return df_all

def maude_monthly_counts_18m(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate event count by month over last 18 months; ensure zero-filled months."""
    start_date, end_date, months = last_18_month_window()
    if df.empty:
        return pd.DataFrame({"month": months.astype(str), "count": [0]*len(months)})

    # Parse dates and restrict to window
    dt = pd.to_datetime(df["date_received"], format="%Y%m%d", errors="coerce")
    mask = (dt >= start_date) & (dt <= end_date)
    df = df.loc[mask].copy()
    if df.empty:
        return pd.DataFrame({"month": months.astype(str), "count": [0]*len(months)})

    month_periods = dt[mask].dt.to_period("M")
    counts = month_periods.value_counts().sort_index()
    # Reindex to full 18-month PeriodIndex
    counts = counts.reindex(months, fill_value=0)
    out = counts.rename_axis("month").reset_index(name="count")
    # For the chart, convert Period to timestamp (month start) to get a nice x-axis
    out["month_ts"] = out["month"].dt.to_timestamp()
    return out[["month_ts", "count"]]

# -------------------- UI --------------------

st.title("FDA Manufacturer Finder")
st.caption("Filter FDA device establishments by **location** and **products** (openFDA). Then pick a supplier to see **MAUDE** events for the last 18 months.")

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
        df = normalize_reglisting_rows(data)

        st.success(f"Found {len(df):,} establishments")
        st.dataframe(df.drop(columns=["Firm Label"]), use_container_width=True)
        st.download_button("Download CSV", df.to_csv(index=False).encode("utf-8"), "fda_mfrs.csv", "text/csv")

        # ---- Select a supplier/manufacturer and show MAUDE over last 18 months
        st.subheader("MAUDE for selected supplier (last 18 months)")
        selected_label = st.selectbox("Choose a manufacturer", df["Firm Label"].tolist())
        sel_row = df[df["Firm Label"] == selected_label].iloc[0]
        firm_name = sel_row["Firm Name"]

        st.caption(f"Looking up MAUDE for: **{firm_name}** (last 18 months)")

        with st.spinner("Fetching MAUDE events…"):
            df_maude = fetch_maude_events_18m(firm_name)

        if df_maude.empty:
            st.warning("No MAUDE events found for this firm in the last 18 months.")
        else:
            counts_18m = maude_monthly_counts_18m(df_maude)
            st.line_chart(
                counts_18m.set_index("month_ts")["count"],
                height=280,
            )
            st.dataframe(df_maude, use_container_width=True)
            st.download_button(
                "Download MAUDE CSV (last 18 months)",
                df_maude.to_csv(index=False).encode("utf-8"),
                "maude_18m.csv",
                "text/csv"
            )
else:
    st.info("Set your filters in the sidebar and click **Search**.")
