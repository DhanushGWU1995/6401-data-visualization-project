import pandas as pd
import os

# Files you uploaded
files = [
    "data/Unemployment Rate - College Graduates - Master's Degree, 20 to 24 years.xlsx",
    "data/Unemployment Rate - College Graduates - Master's Degree, 25 to 34 years.xlsx",
    "data/Unemployment Rate - College Graduates - Master's Degree, 35 to 44 years.xlsx",
    "data/Unemployment Rate - College Graduates - Master's Degree, 45 to 54 years.xlsx",
    "data/Unemployment Rate - College Graduates - Master's Degree, 55 to 64 years .xlsx"
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

# Convert observation_date to Date (keep full date, no time)
combined_df['Date'] = pd.to_datetime(combined_df['observation_date']).dt.date

# Find all CGMD columns and combine them into one Unemployment_Rate column
cgmd_columns = [col for col in combined_df.columns if col.startswith('CGMD')]
combined_df['Unemployment_Rate'] = combined_df[cgmd_columns].bfill(axis=1).iloc[:, 0]

# Keep only the relevant columns
combined_df = combined_df[['Date', 'Unemployment_Rate', 'Age_Group']]

# Remove rows with NaN unemployment rate
combined_df = combined_df.dropna(subset=['Unemployment_Rate'])

# ---------------------------
# Process Job Postings Files
# ---------------------------
job_posting_files = {
    "data/Electrical Engineering Job Postings on Indeed in the United States_2020_2026.xlsx": "Electrical Job Postings USA",
    "data/Industrial Engineering Job Postings on Indeed in the United States_2020_2026.xlsx": "Industrial Job Postings USA",
    "data/Software Development Job Postings on Indeed in the United States_2020_2026.xlsx": "Software Job Postings USA",
    "data/Nursing Job Postings on Indeed in the United States_2020_2026.xlsx": "Nursing Job Postings USA",
    "data/Marketing Job Postings on Indeed in the United States_2020_2026.xlsx": "Marketing Job Postings USA",
    "data/Banking and Finance Job Postings on Indeed in the United States_2020_2026.xlsx": "Banking Job Postings USA"
}

job_posting_dfs = {}

for file_path, sheet_name in job_posting_files.items():
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
    
    # Convert Date to datetime (date only, no time)
    df['Date'] = pd.to_datetime(df['Date']).dt.date
    
    # Store in dictionary
    job_posting_dfs[sheet_name] = df

# ---------------------------
# Save all sheets to Excel
# ---------------------------
output_file = "Combined_Masters_Unemployment_By_Age.xlsx"
with pd.ExcelWriter(output_file, engine='openpyxl', date_format='YYYY-MM-DD', datetime_format='YYYY-MM-DD') as writer:
    combined_df.to_excel(writer, sheet_name='Unemployment Rate USA', index=False)
    
    # Write all job posting sheets
    for sheet_name, df in job_posting_dfs.items():
        df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # Auto-adjust column widths for all sheets
    for sheet_name in writer.sheets:
        worksheet = writer.sheets[sheet_name]
        for column in worksheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            for cell in column:
                try:
                    if len(str(cell.value)) > max_length:
                        max_length = len(str(cell.value))
                except:
                    pass
            adjusted_width = max_length + 2
            worksheet.column_dimensions[column_letter].width = adjusted_width

print("Combined file created:")
print(output_file)
print("\nUnemployment Rate USA - First 5 rows:")
print(combined_df.head())
print("\nJob Posting Sheets Created:")
for sheet_name, df in job_posting_dfs.items():
    print(f"\n{sheet_name} - First 5 rows:")
    print(df.head())
