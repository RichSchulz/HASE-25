import pandas as pd
import statsmodels.formula.api as smf
from pandas.tseries.offsets import DateOffset

# --- 0. Analysis Parameters ---
policy_start_date = '2023-04-01'
window_length = 7  # <-- SET YOUR WINDOW LENGTH (in days) HERE

# --- 1. Load Data (Same as before) ---
try:
    italy_commits = pd.read_csv('../gh-archive-bigquery/output/commits_all_italy_commit_events.csv', parse_dates=['event_timestamp'])
    austria_commits = pd.read_csv('../gh-archive-bigquery/output/commits_all_austria_commit_events.csv', parse_dates=['event_timestamp'])
    france_commits = pd.read_csv('../gh-archive-bigquery/output/commits_all_france_commit_events.csv', parse_dates=['event_timestamp'])
except FileNotFoundError as e:
    print(f"Error: {e}\nStop.")
    exit()

# --- 2. Prepare Data (Modified) ---

# Add 'country' column
italy_commits['country'] = 'Italy'
austria_commits['country'] = 'Austria'
france_commits['country'] = 'France'

# Combine all data
all_commits = pd.concat([italy_commits, austria_commits, france_commits])

# Create time features
all_commits['date'] = all_commits['event_timestamp'].dt.date
all_commits['day_of_week'] = all_commits['event_timestamp'].dt.day_name()

# Aggregate to get counts *for days commits happened*
df_counts = all_commits.groupby(['date', 'country', 'username', 'day_of_week']).size().reset_index(name='commit_count')
df_counts['date'] = pd.to_datetime(df_counts['date']) # Ensure datetime

# --- 3. Create a Complete User-Day Grid ---
# This is essential for a "probability" model, as we need 0s
print("Creating complete user-day grid (this may take a moment)...")

# 1. Get all unique users and their country
unique_users = df_counts[['username', 'country']].drop_duplicates()

# 2. Get all unique dates and their day of week
unique_dates = df_counts[['date', 'day_of_week']].drop_duplicates()

# 3. Create the full grid by crossing users and dates
df_grid = unique_users.merge(unique_dates, how='cross')

# 4. Merge the actual commit counts onto the grid
# Use a left merge to keep all user-day rows, even those with no commits
df_user_day = df_grid.merge(df_counts, 
                            on=['username', 'country', 'date', 'day_of_week'], 
                            how='left')

# 5. Fill NaNs with 0 for days with no commits
df_user_day['commit_count'] = df_user_day['commit_count'].fillna(0)

# 6. Create the new binary outcome variable
df_user_day['did_commit'] = (df_user_day['commit_count'] > 0).astype(int)

print(f"Full user-day grid created. Shape: {df_user_day.shape}")
print(df_user_day.sample(5))
print("-" * 20)

# --- 4. Create DiD Variables & Filter Window ---

# Define the exact start and end dates for the policy window
start_date = pd.to_datetime(policy_start_date)
end_date = start_date + DateOffset(days=window_length - 1) 

print(f"--- Analysis Parameters ---")
print(f"Policy Start Date: {start_date.date()}")
print(f"Window Length: {window_length} days")
print(f"Policy End Date (inclusive): {end_date.date()}")
print("-" * 20)

# 1. Filter the DataFrame to *only* include the pre-period and the post-window
# We now use the complete 'df_user_day' grid
pre_period_data = df_user_day['date'] < start_date
post_window_data = (df_user_day['date'] >= start_date) & (df_user_day['date'] <= end_date)

df_analysis = df_user_day[pre_period_data | post_window_data].copy()

# 2. Create 'post_policy' dummy
df_analysis['post_policy'] = (df_analysis['date'] >= start_date).astype(int)

# 3. Create 'is_treatment' dummy
df_analysis['is_treatment'] = (df_analysis['country'] == 'Italy').astype(int)

print(f"Total observations in analysis: {len(df_analysis)}")
print(f"Min date: {df_analysis['date'].min().date()}")
print(f"Max date: {df_analysis['date'].max().date()}")
print("-" * 20)


# --- 5. Run DiD Regression 1: Commit Count ---
print("\n--- Running DiD Regression 1 (Commit Count) ---")

# This formula is unchanged
formula_count = 'commit_count ~ is_treatment + C(post_policy) + is_treatment:C(post_policy) + C(day_of_week)'

try:
    model_count = smf.ols(formula=formula_count, data=df_analysis).fit()
    print(model_count.summary())

    print("\n--- Interpretation (Commit Count) ---")
    print(f"The 'is_treatment:C(post_policy)[T.1]' coefficient estimates the change")
    print(f"in the *number* of daily commits per user for Italy during the first {window_length} days,")
    print("  compared to the change for the Austria/France group.")

except Exception as e:
    print(f"An error occurred during count model fitting: {e}")

# --- 6. Run DiD Regression 2: Probability of Committing ---
print("\n" + "="*40)
print("--- Running DiD Regression 2 (Probability of Commit) ---")

# New formula for the binary 'did_commit' outcome
formula_prob = 'did_commit ~ is_treatment + C(post_policy) + is_treatment:C(post_policy) + C(day_of_week)'

try:
    # This is a Linear Probability Model (LPM)
    model_prob = smf.ols(formula=formula_prob, data=df_analysis).fit()
    print(model_prob.summary())

    print("\n--- Interpretation (Probability of Commit) ---")
    print(f"The 'is_treatment:C(post_policy)[T.1]' coefficient estimates the change")
    print(f"in the *probability* (from 0 to 1) that a user makes at least one commit per day")
    print(f"for Italy during the first {window_length} days, compared to the Austria/France group.")
    print("e.g., a value of -0.05 would suggest a 5 percentage point drop in probability.")
    
except Exception as e:
    print(f"An error occurred during probability model fitting: {e}")
    print("This can happen if your 'window_length' is too short and results in no data for one of the groups.")
