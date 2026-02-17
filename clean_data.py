#%%
import pandas as pd
import numpy as np

#%%
# 1. only include 2015 - 2024 in census data
census_data = pd.read_csv('../data/census_bds_dump.csv')
census_data = census_data[(census_data['YEAR'] >= 2015) & (census_data['YEAR'] <= 2024)] 
census_data.sort_values(by=['YEAR'], inplace=True)

# %%
# 2. change column name from value to job openings in ces
ces_data = pd.read_csv('../data/ces_all_states_total_all_emp_monthly.csv')
ces_data.rename(columns={'value': 'job_openings'}, inplace=True)
ces_data.sort_values(by=['year'], inplace=True)
ces_data

# %%
# 3. change column name from value to unemployment rate in laus
laus_data = pd.read_csv('../data/laus_all_states_unemp_rate_monthly.csv')
laus_data.rename(columns={'value': 'unemployment_rate'}, inplace=True)
laus_data.sort_values(by=['year'], inplace=True)
laus_data


# %%
# 4. merge laus and ces data on year, period, and state
merged_ces_laus_data = pd.merge(ces_data, laus_data, on=['year', 'period', 'periodName', 'footnotes','state'], how='inner')
merged_ces_laus_data.sort_values(by=['year'], inplace=True)
merged_ces_laus_data

# %%
# 5. save the updated dataframes to new csv files
census_data.to_csv('../data/clean/cleaned_census_bds_dump.csv', index=False)
merged_ces_laus_data.to_csv('../data/clean/cleaned_ces_laus_data.csv', index=False)
# %%
