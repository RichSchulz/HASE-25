#!/usr/bin/env python3
"""
Difference-in-Differences Analysis with Heterogeneity (Activity Volume)
Splits users into Low/Mid/High activity tertiles based on pre-ban behavior.
"""

import pandas as pd
import numpy as np
import sqlite3
import warnings
import gc
from datetime import timedelta

# Check for linearmodels
try:
    from linearmodels.iv import AbsorbingLS
    LINEARMODELS_AVAILABLE = True
except ImportError:
    LINEARMODELS_AVAILABLE = False
    print("CRITICAL WARNING: 'linearmodels' not installed. Falling back to slow statsmodels.")
    from statsmodels.formula.api import ols

warnings.filterwarnings('ignore')

# ================= CONFIGURATION =================
DB_PATH = "/Users/richard/University/HASE-25/scripts/data/data_commits.sqlite3"
OUTPUT_PATH = "/Users/richard/University/HASE-25/final report/parts/did_heterogeneity_table.tex"
MIN_PRE_PERIOD_COMMITS = 0  # Minimum commits required to be included in analysis (0 = include all users)
# NOTE: Set to 0 to include ALL users (matches simple_did_analysis.py sample).
# Set to 5+ to filter to only "active" users. This allows comparison of results with/without
# the activity filter to see if effects are concentrated among active users.
# =================================================

def load_data():
    """Load raw data with SQL filtering to save memory."""
    print("Loading commit data from SQLite database...")
    
    # Date buffer: Load enough data to establish pre-period activity
    # NOTE: We need data from Feb 1 to classify users by pre-ban activity,
    # but we match the simple analysis date range for the analysis window itself
    sql_start_date = '2023-02-01'  # Need earlier start to classify users by pre-ban activity
    sql_end_date = '2023-05-01'

    conn = sqlite3.connect(DB_PATH)
    query = f"""
    SELECT country, username, push_event_timestamp, additions, deletions, changes, commit_sha
    FROM commit_data
    WHERE push_event_timestamp BETWEEN '{sql_start_date}' AND '{sql_end_date}'
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Basic cleanup
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date
    
    # Optimize numeric types
    cols = ['additions', 'deletions', 'changes']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
        
    df = df.dropna(subset=['additions', 'username', 'country', 'date'])
    print(f"Total raw commits loaded: {len(df)}")
    return df

def classify_users_by_activity(df):
    """
    Classifies users into Low, Mid, High activity based on PRE-BAN volume.
    Filters out users with < MIN_PRE_PERIOD_COMMITS.
    """
    print("\n--- Classifying Users by Activity Level ---")
    
    ban_date = pd.Timestamp('2023-04-01').date()
    
    # 1. Isolate Pre-Ban Data
    pre_ban_df = df[df['date'] < ban_date]
    
    # 2. Count commits per user
    user_activity = pre_ban_df.groupby('username')['commit_sha'].count().reset_index()
    user_activity.rename(columns={'commit_sha': 'total_pre_commits'}, inplace=True)
    
    total_users = len(user_activity)
    
    # 3. Filter Noise (users with very few commits) - only if threshold > 0
    if MIN_PRE_PERIOD_COMMITS > 0:
        active_users = user_activity[user_activity['total_pre_commits'] >= MIN_PRE_PERIOD_COMMITS].copy()
        filtered_count = len(active_users)
        dropped_count = total_users - filtered_count
        print(f"Total Users: {total_users}")
        print(f"Dropped (Assume Inactive/Noise < {MIN_PRE_PERIOD_COMMITS} commits): {dropped_count}")
        print(f"Remaining Active Users: {filtered_count}")
    else:
        active_users = user_activity.copy()
        print(f"Total Users: {total_users}")
        print(f"Including ALL users (no minimum commit threshold)")
    
    # 4. Split into Tertiles (Low, Mid, High)
    # qcut partitions data into equal-sized buckets
    try:
        active_users['activity_group'] = pd.qcut(
            active_users['total_pre_commits'], 
            q=3, 
            labels=['Low', 'Mid', 'High']
        )
    except ValueError:
        # Fallback if unique values are too sparse for strict quantiles
        print("Warning: Unique commit counts sparse, using rank method.")
        active_users['activity_group'] = pd.qcut(
            active_users['total_pre_commits'].rank(method='first'), 
            q=3, 
            labels=['Low', 'Mid', 'High']
        )

    # Stats for the groups
    print("\nGroup Thresholds:")
    print(active_users.groupby('activity_group')['total_pre_commits'].describe()[['min', 'max', 'count', 'mean']])
    
    # Convert to string to avoid categorical comparison issues
    active_users['activity_group'] = active_users['activity_group'].astype(str)
    
    return active_users[['username', 'activity_group']]

def prepare_did_variables(df, user_groups, treatment_period='two_weeks', pre_period_weeks=4):
    """Aggregates data and merges user groups.
    
    Parameters match simple_did_analysis.py for consistency:
    - pre_period_weeks: Number of weeks before ban to include (default: 4)
    """
    
    # Filter df to only include classified users
    print(f"\nBefore merge: {len(df):,} rows, {df['username'].nunique():,} unique users")
    print(f"User groups to merge: {len(user_groups):,} users")
    df = df.merge(user_groups, on='username', how='inner')
    print(f"After merge: {len(df):,} rows, {df['username'].nunique():,} unique users")
    print(f"Activity group distribution after merge:")
    print(df['activity_group'].value_counts())
    
    # Date Logic - MATCH simple_did_analysis.py
    april_1st = pd.Timestamp('2023-04-01').date()
    pre_period_start = april_1st - timedelta(weeks=pre_period_weeks)
    
    # Filter Window - MATCH simple_did_analysis.py
    if treatment_period == 'two_weeks':
        end_date = pd.Timestamp('2023-04-15').date()
        mask = (df['date'] >= pre_period_start) & (df['date'] < end_date)
    elif treatment_period == 'four_weekdays':
        end_date = pd.Timestamp('2023-04-07').date()
        mask = (df['date'] >= pre_period_start) & (df['date'] < end_date)
    else:
        raise ValueError(f"Unknown treatment_period: {treatment_period}")
    
    df = df[mask].copy()
    
    # Aggregate to User-Day level
    # Note: activity_group should be the same for all rows of a given username
    # but we include it in groupby to ensure it's preserved
    daily = df.groupby(['username', 'country', 'date', 'activity_group']).agg({
        'additions': 'sum',
        'deletions': 'sum'
    }).reset_index()
    
    # Ensure activity_group is string type for reliable filtering
    daily['activity_group'] = daily['activity_group'].astype(str)
    
    # Convert date to datetime for calculations (needed for weekday check and time trends)
    daily['date_dt'] = pd.to_datetime(daily['date'])
    
    # DiD Setup - MATCH simple_did_analysis.py
    daily['treatment'] = (daily['country'].str.lower() == 'italy').astype(int)
    
    # Post-treatment logic - MATCH simple_did_analysis.py
    if treatment_period == 'two_weeks':
        end_post = pd.Timestamp('2023-04-15').date()
        daily['post_treatment'] = ((daily['date'] >= april_1st) & 
                                    (daily['date'] < end_post)).astype(int)
    elif treatment_period == 'four_weekdays':
        start_post = pd.Timestamp('2023-04-03').date()
        end_post = pd.Timestamp('2023-04-07').date()
        daily['is_weekday'] = (daily['date_dt'].dt.dayofweek < 5).astype(int)
        daily['post_treatment'] = ((daily['date'] >= start_post) & 
                                   (daily['date'] < end_post) &
                                   (daily['is_weekday'] == 1)).astype(int)
    
    daily['treatment_post'] = daily['treatment'] * daily['post_treatment']
    
    # Time Trend (for Linearmodels)
    min_date = daily['date_dt'].min()
    daily['days_since_start'] = (daily['date_dt'] - min_date).dt.days
    daily['treatment_time_trend'] = daily['treatment'] * daily['days_since_start']
    
    # Log Outcomes
    daily['log_additions'] = np.log1p(daily['additions'])
    daily['log_deletions'] = np.log1p(daily['deletions'])
    
    # === CRITICAL FIX: Convert Fixed Effects columns to Category ===
    daily['username'] = daily['username'].astype('category')
    daily['date'] = daily['date'].astype('category')
    
    return daily

def run_regression(df, outcome_var):
    """Runs AbsorbingLS Fixed Effects Model."""
    if LINEARMODELS_AVAILABLE:
        # Absorb User and Date Fixed Effects
        absorb_cols = ['username', 'date']
        exog_vars = ['treatment_post', 'treatment_time_trend']
        
        # Remove any rows with missing values in key variables
        df_clean = df[[outcome_var] + exog_vars + absorb_cols].dropna()
        
        if len(df_clean) == 0:
            print(f"  WARNING: No valid observations after cleaning!")
            return None
        
        y = df_clean[outcome_var]
        X = df_clean[exog_vars]
        
        try:
            model = AbsorbingLS(y, X, absorb=df_clean[absorb_cols])
            results = model.fit(cov_type='clustered', clusters=df_clean[['username']])
            
            # Store actual observation count from cleaned data
            # (linearmodels may report different nobs due to FE absorption)
            results._actual_nobs = len(df_clean)
            
            return results
        except Exception as e:
            print(f"Regression failed: {e}")
            return None
    else:
        return None

def export_heterogeneity_table(results_map, filepath, treatment_period='two_weeks'):
    """
    Creates a LaTeX table comparing Low, Mid, High groups.
    results_map structure: {(outcome, group, treatment_period): result_object}
    """
    print(f"\nExporting table to {filepath} (treatment period: {treatment_period})")
    
    groups = ['Low', 'Mid', 'High']
    outcomes = ['log_additions', 'log_deletions']
    
    with open(filepath, 'w') as f:
        f.write(r"\begin{table}[htbp]" + "\n")
        f.write(r"\centering" + "\n")
        f.write(r"\small" + "\n")
        f.write(r"\caption{Heterogeneity Analysis by Developer Activity Level}" + "\n")
        f.write(r"\label{tab:heterogeneity}" + "\n")
        f.write(r"\begin{tabular}{lccc|ccc}" + "\n")
        f.write(r"\hline \hline" + "\n")
        
        # Header
        f.write(r" & \multicolumn{3}{c}{\textbf{Log Additions}} & \multicolumn{3}{c}{\textbf{Log Deletions}} \\" + "\n")
        f.write(r" & Low & Mid & High & Low & Mid & High \\" + "\n")
        f.write(r"\hline" + "\n")
        
        # Build coefficient row
        row_coef = "Italy $\\times$ Post"
        for outcome in outcomes:
            for group in groups:
                res = results_map.get((outcome, group, treatment_period))
                if res:
                    val = res.params['treatment_post']
                    pval = res.pvalues['treatment_post']
                    
                    stars = ""
                    if pval < 0.01: stars = "***"
                    elif pval < 0.05: stars = "**"
                    elif pval < 0.1: stars = "*"
                    
                    row_coef += f" & {val:.4f}$^{{{stars}}}$"
                else:
                    row_coef += " & -"
        
        f.write(row_coef + r" \\" + "\n")
        
        # Build standard errors row (empty first column to align with coefficient row)
        row_se = ""
        for outcome in outcomes:
            for group in groups:
                res = results_map.get((outcome, group, treatment_period))
                if res:
                    se = res.std_errors['treatment_post']
                    row_se += f" & ({se:.4f})"
                else:
                    row_se += " & -"
        
        f.write(row_se + r" \\" + "\n")
        f.write(r"\hline" + "\n")
        
        # Observations Row
        # Use actual observation count from dataframe if available, otherwise use res.nobs
        obs_str = "Observations"
        for outcome in outcomes:
            for group in groups:
                res = results_map.get((outcome, group, treatment_period))
                if res:
                    # Prefer manually stored count, fall back to res.nobs
                    nobs = getattr(res, '_actual_nobs', None) or res.nobs
                    obs_str += f" & {int(nobs):,}"
                else:
                    obs_str += " & -"
        f.write(obs_str + r" \\" + "\n")
        
        f.write(r"User FE & Yes & Yes & Yes & Yes & Yes & Yes \\" + "\n")
        f.write(r"Date FE & Yes & Yes & Yes & Yes & Yes & Yes \\" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r"\end{tabular}" + "\n")
        f.write(r"\vspace{0.1cm}" + "\n")
        f.write(r"\begin{flushleft}" + "\n")
        f.write(r"\footnotesize" + "\n")
        f.write(r"\textit{Notes:} Activity groups defined by pre-ban commit volume tertiles. ")
        f.write(f"Users with fewer than {MIN_PRE_PERIOD_COMMITS} pre-ban commits excluded. ")
        f.write(r"Standard errors clustered by user in parentheses. ")
        f.write(r"*** p$<$0.01, ** p$<$0.05, * p$<$0.1." + "\n")
        f.write(r"\end{flushleft}" + "\n")
        f.write(r"\end{table}" + "\n")

def main():
    # 1. Load
    df_raw = load_data()
    
    # 2. Classify Users (The Heterogeneity Logic)
    print("\n" + "="*60)
    if MIN_PRE_PERIOD_COMMITS > 0:
        print("IMPORTANT: This analysis filters to only active users")
        print(f"(>= {MIN_PRE_PERIOD_COMMITS} pre-ban commits)")
        print("This differs from simple_did_analysis.py which includes ALL users.")
    else:
        print("NOTE: Including ALL users (no minimum commit threshold)")
        print("This matches the sample used in simple_did_analysis.py")
    print("="*60)
    user_groups = classify_users_by_activity(df_raw)
    
    results_map = {}
    groups = ['Low', 'Mid', 'High']
    outcomes = ['log_additions', 'log_deletions']
    treatment_periods = ['two_weeks', 'four_weekdays']
    
    # 3. Run analysis for each treatment period
    for treatment_period in treatment_periods:
        print(f"\n{'='*60}")
        print(f"TREATMENT PERIOD: {treatment_period}")
        print(f"{'='*60}")
        
        # Prepare Data with classification
        # Use same time window as simple_did_analysis.py (4 weeks pre-period)
        df_analysis = prepare_did_variables(df_raw, user_groups, treatment_period=treatment_period, pre_period_weeks=4)
        
        print(f"\nPrepared analysis dataset ({treatment_period}):")
        print(f"  Total rows: {len(df_analysis):,}")
        print(f"  Unique users: {df_analysis['username'].nunique():,}")
        print(f"  Unique dates: {df_analysis['date'].nunique()}")
        print(f"  Rows by group:")
        print(df_analysis.groupby('activity_group').size())
        print(f"  Unique users by group:")
        print(df_analysis.groupby('activity_group')['username'].nunique())
        
        # 4. Iterate and Regress
        print(f"\n--- Running Regressions per Group ({treatment_period}) ---")
        for group in groups:
            print(f"\nProcessing Group: {group}")
            
            # Filter for specific group (ensure string comparison)
            group_str = str(group)
            df_subset = df_analysis[df_analysis['activity_group'].astype(str) == group_str].copy()
            
            print(f"     Filtered to group '{group_str}': {len(df_subset):,} rows")
            
            if df_subset.empty:
                print("No data for this group!")
                continue
                
            for outcome in outcomes:
                print(f"  -> Outcome: {outcome}")
                print(f"     Subset size: {len(df_subset)} rows")
                print(f"     Unique users: {df_subset['username'].nunique()}")
                print(f"     Unique dates: {df_subset['date'].nunique()}")
                res = run_regression(df_subset, outcome)
                if res:
                    # Store with treatment period in key
                    results_map[(outcome, group, treatment_period)] = res
                    print(f"     Coef: {res.params['treatment_post']:.4f} (p={res.pvalues['treatment_post']:.4f})")
                    print(f"     Regression obs (nobs): {int(res.nobs):,}")

    # 5. Export (default to two_weeks, but you can change this to 'four_weekdays' if needed)
    export_heterogeneity_table(results_map, OUTPUT_PATH, treatment_period='two_weeks')
    print("\nAnalysis Complete.")
    print(f"\nNote: Table exported for 'two_weeks' period. To export 'four_weekdays',")
    print(f"      modify the export_heterogeneity_table call in main().")

if __name__ == "__main__":
    main()
