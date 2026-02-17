import os, json, requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
BLS_KEY = os.getenv("BLS_API_KEY")

STATE_FIPS = {
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

BASE_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
OUT_DIR = "data/laus_states"
os.makedirs(OUT_DIR, exist_ok=True)

YEAR_WINDOWS = [("2015", "2024")]  # keep <= 10 years per request

def fetch_series(series_id: str, startyear: str, endyear: str) -> pd.DataFrame:
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
        timeout=60
    )
    r.raise_for_status()
    j = r.json()

    series = j["Results"]["series"][0]
    return pd.DataFrame(series.get("data", []))

all_states_dfs = []

for st, fips in STATE_FIPS.items():
    # correct series id format: 11 zeros + 003
    series_id = f"LAUST{fips}0000000000003"  # unemployment rate

    parts = []
    for sy, ey in YEAR_WINDOWS:
        df_part = fetch_series(series_id, sy, ey)
        if not df_part.empty:
            parts.append(df_part)

    if not parts:
        print(f"No data for {st} ({series_id})")
        continue

    df = pd.concat(parts, ignore_index=True)

    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["state"] = st
    df["series_id"] = series_id

    # save per-state
    per_state_path = os.path.join(OUT_DIR, f"{st}_unemp_rate_monthly.csv")
    df.to_csv(per_state_path, index=False)
    print(f"Saved {per_state_path} ({len(df)} rows)")

    # store for combined
    all_states_dfs.append(df)

# combined save
combined = pd.concat(all_states_dfs, ignore_index=True)
combined = combined.sort_values(["state", "year", "period"])
combined_out = "data/laus_all_states_unemp_rate_monthly.csv"
combined.to_csv(combined_out, index=False)
print(f"Saved {combined_out} ({len(combined)} rows)")