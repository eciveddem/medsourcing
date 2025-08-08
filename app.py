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

# ---------- Session defaults ----------
if "df_regs" not in st.session_state:
    st.session_state.df_regs = None
if "search_params" not in st.session_state:
    st.session_state.search_params = {}
if "selected_label" not in st.session_state:
    st.session_state.selected_label = None

# ---------- Helpers ----------
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
    query = f"search=device_name:{quote_plus(q)}&limit={limit}"
    url = f"{CLASS_ENDPOINT}?{query}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        return []
    data = r.json()
    results = data.get("results", [])
    codes = sorted({rec.get("product_code") for rec in results if rec.get("product_code")})
    return codes

def build_reglisting_search(iso2: str, product_codes: list[str]) -> str:
    """
    Country AND (code1 OR code2 OR ...)
    Use .exact for product codes so 3-letter codes match precisely.
    """
    parts = []
    if iso2:
        parts.append(f"registration.iso_country_code:{iso2}")

    pcs = [pc.strip().upper() for pc in (product_codes or []) if pc and pc.strip()]
    if len(pcs) == 1:
        parts.append(f"products.product_code.exact:{pcs[0]}")
    elif len(pcs) > 1:
        or_group = "+OR+".join([f"products.product_code.exact:{pc}" for pc in pcs])
        parts.append(f"({or_group})")

    return "+".join(parts) if parts else ""

@st.cache_data(show_spinner=True)
def fetch_reglisting(iso2: str, product_codes: list[str], max_records=2000):
    rows, limit, skip, fetched = [], 1000, 0, 0
    search = build_reglisting_search(iso2, product_codes)

    while fetched < max_records:
        params = {"search": search, "limit": limit, "skip": skip} if search else {"limit": limit, "skip": skip}
        r = requests.get(REG_LISTING_ENDPOINT, params=params, timeout=60)
        if r.status_code != 200:
            break
        payload = r.json() or {}
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

def normalize_reglisting_rows(rows: list[dict]) -> pd.DataFrame:
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
    df["Firm Label"] = df.apply(
        lambda x: f'{x["Firm Name"]} — {x["City"] or ""} {x["State/Prov"] or ""} ({x["Country"]})',
        axis=1
    )
    return df

# ----- MAUDE (last 18 months) -----
def last_18_month_window() -> tuple[pd.Timestamp, pd.Timestamp, pd.PeriodIndex]:
    today = pd.Timestamp.today().normalize()
    months = pd.period_range(end=today.to_period("M"), periods=18, freq="M")
    start_date = months[0].to_timestamp(how="start")
    end_date = months[-1].to_timestamp(how="end")
    return start_date, end_date, months

def build_maude_queries(firm_name: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> list[str]:
    date_clause = f'date_received:[{start_date:%Y%m%d}+TO+{end_date:%Y%m%d}]'
    # Two avenues (either can match depending on record)
    a = f'manufacturer_name:"{firm_name}"+{date_clause}'
    b = f'device.manufacturer_d_name:"{firm_name}"+{date_clause}'
    return [a, b]

@st.cache_data(show_spinner=True)
def fetch_maude_events_18m(firm_name: str, max_records: int = 5000) -> tuple[pd.DataFrame, list[str]]:
    start_date, end_date, _ = last_18_month_window()
    queries = build_maude_queries(firm_name, start_date, end_date)

    frames = []
    limit = 1000
    preview_urls = []
    for q in queries:
        fetched = 0
        skip = 0
        while fetched < max_records:
            params = {"search": q, "limit": limit, "skip": skip}
            prepared = requests.Request("GET", MAUDE_ENDPOINT, params=params).prepare().url
            if skip == 0:  # only show first page per query in preview
                preview_urls.append(prepared)
            r = requests.get(MAUDE_ENDPOINT, params=params, timeout=60)
            if r.status_code != 200:
                break
            payload = r.json() or {}
            results = payload.get("results", [])
            if not results:
                break
            frames.append(pd.json_normalize(results))
            n = len(results)
            fetched += n
            if n < limit:
                break
            skip += n

    if not frames:
        return pd.DataFrame(columns=["date_received"]), preview_urls
    df = pd.concat(frames, ignore_index=True)
    if "date_received" not in df.columns:
        df["date_received"] = pd.Series(dtype="object")
    return df, preview_urls

def maude_monthly_counts_18m(df: pd.DataFrame) -> pd.DataFrame:
    start_date, end_date, months = last_18_month_window()
    if df.empty:
        return pd.DataFrame({"month_ts": months.to_timestamp(), "count": [0]*len(months)})

    dt = pd.to_datetime(df["date_received"], format="%Y%m%d", errors="coerce")
    mask = (dt >= start_date) & (dt <= end_date)
    dt = dt[mask]
    if dt.empty:
        return pd.DataFrame({"month_ts": months.to_timestamp(), "count": [0]*len(months)})

    counts = dt.dt.to_period("M").value_counts().sort_index()
    counts = counts.reindex(months, fill_value=0)
    out = counts.rename_axis("month").reset_index(name="count")
    out["month_ts"] = out["month"].dt.to_timestamp()
    return out[["month_ts", "count"]]

# ---------- UI: Filters in a FORM ----------
st.title("FDA Manufacturer Finder")
st.caption("Filter FDA device establishments by **country** and **product code(s)**, then pick a supplier to see **MAUDE** events for the last 18 months.")

with st.sidebar:
    with st.form("filters_form", clear_on_submit=False):
        st.header("Filters")
        country_input = st.text_input("Country (name or ISO-2)",
                                      value=st.session_state.search_params.get("country_input", "United States"))
        iso2 = country_to_iso2(country_input) or ""

        mode = st.radio("Search by:", ["Product code(s)", "Device name"], horizontal=True,
                        index=0 if st.session_state.search_params.get("mode","Product code(s)")=="Product code(s)" else 1)

        product_codes = []
        device_name = ""
        if mode == "Product code(s)":
            pcs_default = st.session_state.search_params.get("pcs", "")
            pcs = st.text_input("Product code(s), comma-separated",
                                value=pcs_default, placeholder="e.g., DQD, FMF")
            product_codes = [p.strip().upper() for p in pcs.split(",") if p.strip()]
        else:
            device_name = st.text_input("Device name",
                                        value=st.session_state.search_params.get("device_name",""),
                                        placeholder="e.g., pulse oximeter")
            if device_name:
                with st.spinner("Finding product codes..."):
                    product_codes = lookup_product_codes_by_name(device_name)

        st.write("Resolved product codes:", ", ".join(product_codes) if product_codes else "—")
        max_records = st.slider("Max registrations to pull", 100, 5000,
                                st.session_state.search_params.get("max_records", 2000), 100)

        submitted = st.form_submit_button("Search", type="primary")

    # Persist inputs
    st.session_state.search_params = {
        "country_input": country_input,
        "mode": mode,
        "pcs": ", ".join(product_codes) if mode == "Product code(s)" else st.session_state.search_params.get("pcs",""),
        "device_name": device_name if mode == "Device name" else "",
        "max_records": max_records,
    }

# ---------- Run search only when submitted ----------
if submitted:
    iso2 = country_to_iso2(st.session_state.search_params["country_input"]) or ""
    pcs_for_query = ([p.strip() for p in st.session_state.search_params.get("pcs","").split(",") if p.strip()]
                     if st.session_state.search_params["mode"] == "Product code(s)" else
                     lookup_product_codes_by_name(st.session_state.search_params.get("device_name","")))
    with st.spinner("Querying openFDA Registrations…"):
        rows = fetch_reglisting(iso2, pcs_for_query, max_records=st.session_state.search_params["max_records"])
    st.session_state.df_regs = normalize_reglisting_rows(rows) if rows else pd.DataFrame()
    st.session_state.selected_label = None  # reset selection after a new search

# ---------- Show results ----------
df_regs = st.session_state.df_regs
iso2_for_preview = country_to_iso2(st.session_state.search_params.get("country_input","")) or ""
pcs_preview = ([p.strip() for p in st.session_state.search_params.get("pcs","").split(",") if p.strip()]
               if st.session_state.search_params.get("mode")=="Product code(s)" else
               lookup_product_codes_by_name(st.session_state.search_params.get("device_name","")))
reg_preview_params = {"search": build_reglisting_search(iso2_for_preview, pcs_preview), "limit": 5, "skip": 0}
reg_preview_url = requests.Request("GET", REG_LISTING_ENDPOINT, params=reg_preview_params).prepare().url
st.caption("Registration query preview:")
st.code(reg_preview_url, language="text")

if df_regs is None:
    st.info("Use the sidebar to search.")
elif df_regs.empty:
    st.warning("No results. Try a different country or product selection.")
else:
    st.success(f"Found {len(df_regs):,} establishments")
    st.dataframe(df_regs.drop(columns=["Firm Label"]), use_container_width=True)
    st.download_button("Download CSV", df_regs.to_csv(index=False).encode("utf-8"), "fda_mfrs.csv", "text/csv")

    st.subheader("MAUDE for selected supplier (last 18 months)")
    labels = df_regs["Firm Label"].tolist()
    default_index = labels.index(st.session_state.selected_label) if st.session_state.selected_label in labels else 0
    selected_label = st.selectbox("Choose a manufacturer", labels, index=default_index, key="selected_label")

    if selected_label:
        sel_row = df_regs[df_regs["Firm Label"] == selected_label].iloc[0]
        firm_name = sel_row["Firm Name"]
        st.caption(f"MAUDE for **{firm_name}** — last 18 months")

        try:
            with st.spinner("Fetching MAUDE events…"):
                df_maude, maude_preview_urls = fetch_maude_events_18m(firm_name)
            # Preview the first page(s) of the MAUDE queries used
            st.caption("MAUDE query preview(s):")
            for url in maude_preview_urls:
                st.code(url, language="text")

            monthly = maude_monthly_counts_18m(df_maude)
            st.line_chart(monthly.set_index("month_ts")["count"], height=300)
            st.dataframe(df_maude, use_container_width=True)
            st.download_button(
                "Download MAUDE CSV (last 18 months)",
                df_maude.to_csv(index=False).encode("utf-8"),
                "maude_18m.csv",
                "text/csv"
            )
        except Exception as e:
            st.error(f"MAUDE lookup failed: {e}")
