import os, json, requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
BLS_KEY = os.getenv("BLS_API_KEY")
if not BLS_KEY:
    raise RuntimeError("Missing BLS_API_KEY in your .env")

BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# CES uses BLS "state codes" (same numbers you're already using for LAUS FIPS-like state codes)
# (Includes DC; excludes territories by default)
STATE_CODES = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
    "CO": "08", "CT": "09", "DE": "10", "DC": "11", "FL": "12",
    "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
    "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23",
    "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38",
    "OH": "39", "OK": "40", "OR": "41", "PA": "42", "RI": "44",
    "SC": "45", "SD": "46", "TN": "47", "TX": "48", "UT": "49",
    "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
    "WY": "56",
}

# CES State & Metro series pieces
SEASONAL = "U"         # U = Not seasonally adjusted
AREA = "00000"         # statewide
INDUSTRY = "00000000"  # Total Nonfarm
DATA_TYPE = "01"       # All Employees, In Thousands

OUT_DIR = "data/ces_states"
os.makedirs(OUT_DIR, exist_ok=True)

# BLS API requests should be <= 10 years; so chunk
YEAR_WINDOWS = [("2015", "2024")]  # adjust as needed (<= 10-year span)

def build_series_id(state_code: str) -> str:
    # Example: SMU06000000000000001
    return f"SM{SEASONAL}{state_code}{AREA}{INDUSTRY}{DATA_TYPE}"

def fetch_window(series_id: str, startyear: str, endyear: str) -> pd.DataFrame:
    payload = {
        "seriesid": [series_id],
        "startyear": startyear,
        "endyear": endyear,
        "registrationkey": BLS_KEY,
        "catalog": False,
    }
    r = requests.post(
        BASE_URL,
        headers={"Content-type": "application/json"},
        data=json.dumps(payload),
        timeout=60,
    )
    r.raise_for_status()
    j = r.json()

    msgs = j.get("message", [])
    if msgs:
        print(f"{series_id} messages: {msgs}")

    series = j.get("Results", {}).get("series", [])
    if not series:
        return pd.DataFrame()

    return pd.DataFrame(series[0].get("data", []))

all_states = []

for st, sc in STATE_CODES.items():
    series_id = build_series_id(sc)

    parts = []
    for sy, ey in YEAR_WINDOWS:
        df_part = fetch_window(series_id, sy, ey)
        if not df_part.empty:
            parts.append(df_part)

    if not parts:
        print(f"No data for {st} ({series_id})")
        continue

    df = pd.concat(parts, ignore_index=True)

    # Clean + label
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["state"] = st
    df["series_id"] = series_id

    # CES is monthly; keep period like M01..M12, drop annual averages if you want:
    # df = df[df["period"].str.startswith("M")]

    # Save per-state
    per_state_path = os.path.join(OUT_DIR, f"{st}_ces_total_nonfarm_all_emp_monthly.csv")
    df.to_csv(per_state_path, index=False)
    print(f"Saved {per_state_path} ({len(df)} rows)")

    all_states.append(df)

# Save combined
combined = pd.concat(all_states, ignore_index=True).sort_values(["state", "year", "period"])
combined_out = "data/ces_all_states_total_all_emp_monthly.csv"
combined.to_csv(combined_out, index=False)
print(f"Saved {combined_out} ({len(combined)} rows)")