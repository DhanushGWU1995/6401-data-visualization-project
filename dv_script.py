import pandas as pd
import os

# Files you uploaded
files = [
    "input_data/Unemployment Rate - College Graduates - Master's Degree, 20 to 24 years.xlsx",
    "input_data/Unemployment Rate - College Graduates - Master's Degree, 25 to 34 years.xlsx",
    "input_data/Unemployment Rate - College Graduates - Master's Degree, 35 to 44 years.xlsx",
    "input_data/Unemployment Rate - College Graduates - Master's Degree, 45 to 54 years.xlsx",
    "input_data/Unemployment Rate - College Graduates - Master's Degree, 55 to 64 years .xlsx"
]

all_data = []

for file_path in files:
    
    file_name = os.path.basename(file_path)
    age_group = file_name.split(",")[-1].replace(".xlsx", "").strip()
    
    # ---------------------------
    # Read Excel from 'Monthly' sheet with first row as header
    # ---------------------------
    df = pd.read_excel(file_path, sheet_name='Monthly', header=0)
    
    # Remove empty rows
    df = df.dropna(how="all")
    
    # Add Age Group
    df["Age_Group"] = age_group
    
    all_data.append(df)

# Combine all
combined_df = pd.concat(all_data, ignore_index=True)

# Convert observation_date to Date
combined_df['Date'] = pd.to_datetime(combined_df['observation_date'])

# Find all CGMD columns and combine them into one Unemployment_Rate column
cgmd_columns = [col for col in combined_df.columns if col.startswith('CGMD')]
combined_df['Unemployment_Rate'] = combined_df[cgmd_columns].bfill(axis=1).iloc[:, 0]

# Extract Month, Day, and Year from Date
combined_df['Month'] = combined_df['Date'].dt.month
combined_df['Day'] = combined_df['Date'].dt.day
combined_df['Year'] = combined_df['Date'].dt.year

# Convert Date to date only (no time)
combined_df['Date'] = combined_df['Date'].dt.date

# Keep only the relevant columns
combined_df = combined_df[['Date', 'Month', 'Day', 'Year', 'Unemployment_Rate', 'Age_Group']]

# Remove rows with NaN unemployment rate
combined_df = combined_df.dropna(subset=['Unemployment_Rate'])

# ---------------------------
# Process Job Postings Files
# ---------------------------
job_posting_files = {
    "input_data/Electrical Engineering Job Postings on Indeed in the United States_2020_2026.xlsx": "Electrical Engineering",
    "input_data/Industrial Engineering Job Postings on Indeed in the United States_2020_2026.xlsx": "Industrial Engineering",
    "input_data/Software Development Job Postings on Indeed in the United States_2020_2026.xlsx": "Software Development",
    "input_data/Nursing Job Postings on Indeed in the United States_2020_2026.xlsx": "Nursing",
    "input_data/Marketing Job Postings on Indeed in the United States_2020_2026.xlsx": "Marketing",
    "input_data/Banking and Finance Job Postings on Indeed in the United States_2020_2026.xlsx": "Banking and Finance"
}

all_job_postings = []

for file_path, domain_name in job_posting_files.items():
    # Read the second sheet (Daily, 7-Day)
    df = pd.read_excel(file_path, sheet_name=1, header=0)
    
    # Get the column names
    date_col = df.columns[0]
    value_col = df.columns[1]
    
    # Rename columns to Date and Value
    df = df.rename(columns={date_col: 'Date', value_col: 'Value'})
    
    # Keep only Date and Value columns
    df = df[['Date', 'Value']]
    
    # Remove rows with NaN values
    df = df.dropna()
    
    # Convert Date to datetime
    df['Date'] = pd.to_datetime(df['Date'])
    
    # Add Domain column
    df['Domain'] = domain_name
    
    # Append to list
    all_job_postings.append(df)

# Combine all job postings into one dataframe
combined_job_postings = pd.concat(all_job_postings, ignore_index=True)

# Extract Month, Day, and Year from Date
combined_job_postings['Month'] = combined_job_postings['Date'].dt.month
combined_job_postings['Day'] = combined_job_postings['Date'].dt.day
combined_job_postings['Year'] = combined_job_postings['Date'].dt.year

# Convert Date to date only (no time)
combined_job_postings['Date'] = combined_job_postings['Date'].dt.date

# Reorder columns to have Month, Day, and Year after Date
combined_job_postings = combined_job_postings[['Date', 'Month', 'Day', 'Year', 'Domain', 'Value']]

# ---------------------------
# Save to CSV files
# ---------------------------
# Create output_data directory if it doesn't exist
os.makedirs('output_data', exist_ok=True)

unemployment_output = "output_data/Unemployment_Rate_USA.csv"
job_postings_output = "output_data/Job_Postings_USA.csv"

# Save Unemployment Rate data
combined_df.to_csv(unemployment_output, index=False)

# Save Job Postings data
combined_job_postings.to_csv(job_postings_output, index=False)

print("CSV files created:")
print(f"1. {unemployment_output}")
print(f"2. {job_postings_output}")
print("\nUnemployment Rate USA - First 5 rows:")
print(combined_df.head())
print(f"\nJob Postings USA - Total rows: {len(combined_job_postings)}")
print("First 10 rows:")
print(combined_job_postings.head(10))
print("\nDomains included:")
print(combined_job_postings['Domain'].unique())
