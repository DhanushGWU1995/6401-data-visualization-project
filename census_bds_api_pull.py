import requests
import json
from dotenv import load_dotenv
import os
import pandas as pd

url = "https://api.census.gov/data/timeseries/bds"

CENSUS_BDS_KEY = os.getenv('CENSUS_API_KEY')

vars = "YEAR,STATE,EMP,INDGROUP,INDLEVEL,JOB_CREATION,JOB_CREATION_BIRTHS,JOB_CREATION_CONTINUERS,JOB_CREATION_RATE,JOB_CREATION_BIRTHS,JOB_DESTRUCTION,JOB_DESTRUCTION_DEATHS,JOB_DESTRUCTION_CONTINUERS,JOB_DESTRUCTION_RATE,JOB_DESTRUCTION_RATE_DEATHS,METRO,NET_JOB_CREATION,NET_JOB_CREATION_RATE,NET_JOB_CREATION_RATE,REALLOCATION_RATE"

params = {
    "get": vars,
    "for": "state:*",
    "key": CENSUS_BDS_KEY
}

response = requests.get(url, params=params)
data = response.json()

df = pd.DataFrame(data[1:], columns=data[0])
os.makedirs("data/raw", exist_ok=True)
df.to_csv("data/raw/census_bds_dump.csv", index=False)
print("Saved to data/raw/census_bds_dump.csv")

# NOTE: For variable descriptions, use the following link: https://api.census.gov/data/timeseries/bds/variables.html
