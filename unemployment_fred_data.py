import requests
import pandas as pd
import os

API_KEY = "1fc4fedd3a21ce719e09dd326588706b"

# Define all series to fetch with their age groups
series_config = {
    "CGMD2024": "20 to 24 years",
    "CGMD2534": "25 to 34 years",
    "CGMD3544": "35 to 44 years",
    "CGMD4554": "45 to 54 years",
    "CGMD5564": "55 to 64 years"
}

# Create output_data directory if it doesn't exist
os.makedirs('output_data', exist_ok=True)
OUTPUT_FILE = "output_data/FRED_Unemployment_Rate_Combined.csv"

url = "https://api.stlouisfed.org/fred/series/observations"

all_data = []

# Fetch data for each series
for series_id, age_group in series_config.items():
    print(f"Fetching data for {age_group} ({series_id})...")
    
    params = {
        "series_id": series_id,
        "api_key": API_KEY,
        "file_type": "json"
    }
    
    response = requests.get(url, params=params)
    
    # Check if request was successful
    if response.status_code != 200:
        print(f"Error: API request failed for {series_id} with status code {response.status_code}")
        print(f"Response: {response.text}")
        continue
    
    data = response.json()
    
    # Convert to DataFrame
    df = pd.DataFrame(data["observations"])
    df = df[["date", "value"]]
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    
    # Rename columns for clarity
    df = df.rename(columns={"date": "Date", "value": "Unemployment_Rate"})
    
    # Add Age_Group column
    df['Age_Group'] = age_group
    
    all_data.append(df)
    print(f"  ✓ Fetched {len(df)} records")

# Combine all data
print("\nCombining all series...")
combined_df = pd.concat(all_data, ignore_index=True)

# Convert Date to datetime
combined_df['Date'] = pd.to_datetime(combined_df['Date'])

# Extract Month, Day, and Year
combined_df['Month'] = combined_df['Date'].dt.month_name()
combined_df['Day'] = combined_df['Date'].dt.day
combined_df['Year'] = combined_df['Date'].dt.year

# Convert Date to date only (no time)
combined_df['Date'] = combined_df['Date'].dt.date

# Reorder columns
combined_df = combined_df[['Date', 'Month', 'Day', 'Year', 'Unemployment_Rate', 'Age_Group']]

# Remove rows with NaN unemployment rate
combined_df = combined_df.dropna(subset=['Unemployment_Rate'])

# Sort by Date and Age_Group
combined_df = combined_df.sort_values(['Date', 'Age_Group']).reset_index(drop=True)

# Save to CSV
combined_df.to_csv(OUTPUT_FILE, index=False)

print(f"\n{'='*60}")
print("Combined dataset saved:", OUTPUT_FILE)
print(f"Total records: {len(combined_df)}")
print(f"Date range: {combined_df['Date'].min()} to {combined_df['Date'].max()}")
print(f"\nAge groups included: {combined_df['Age_Group'].nunique()}")
for age_group in sorted(combined_df['Age_Group'].unique()):
    count = len(combined_df[combined_df['Age_Group'] == age_group])
    print(f"  - {age_group}: {count} records")
print("\nFirst 10 rows:")
print(combined_df.head(10))
