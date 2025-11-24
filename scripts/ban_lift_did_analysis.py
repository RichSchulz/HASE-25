#!/usr/bin/env python3
"""
Difference-in-Differences Analysis: Post-Ban Lift (Optimized)
Analyzes commit activity changes after the ChatGPT ban was lifted (April 28, 2023),
comparing activity during the ban with activity after the ban was lifted.
Italy as treatment group and Austria/France as control groups.
Uses absorbed fixed effects (user and date) with clustered standard errors by user.
"""

import pandas as pd
import numpy as np
import sqlite3
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
import warnings
import gc

# OPTIMIZATION: Use linearmodels for efficient Fixed Effects
try:
    from linearmodels.iv import AbsorbingLS
    LINEARMODELS_AVAILABLE = True
except ImportError:
    LINEARMODELS_AVAILABLE = False
    print("CRITICAL WARNING: 'linearmodels' not installed.")
    print("Please run: pip install linearmodels")
    print("Falling back to slow statsmodels (Script may crash on large data).")
    from statsmodels.formula.api import ols

warnings.filterwarnings('ignore')

def load_and_prepare_data(ban_period_weeks=4):
    """
    OPTIMIZATION: Filter by date inside SQL to avoid loading massive history.
    Ban lift date is 2023-04-28. We need roughly March 1st to May 15th.
    """
    print("Loading commit data from SQLite database...")
    
    db_path = "large_data/data_commits.sqlite3"
    
    # Calculate safe date buffer for SQL (give extra weeks to be safe)
    # Ban lift date is 2023-04-28. We need roughly March 1st to May 15th.
    # We filter strictly later in pandas, this is just to save RAM on load.
    sql_start_date = '2023-02-15' 
    sql_end_date = '2023-05-20'

    conn = sqlite3.connect(db_path)
    
    # OPTIMIZATION: WHERE clause added
    query = f"""
    SELECT 
        country,
        username,
        push_event_timestamp,
        additions,
        deletions,
        changes,
        commit_sha
    FROM commit_data
    WHERE push_event_timestamp BETWEEN '{sql_start_date}' AND '{sql_end_date}'
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Total commits loaded (filtered by date in SQL): {len(df)}")
    return df

def prepare_did_variables(df, treatment_period='two_weeks', ban_period_weeks=4):
    """
    Prepare variables for difference-in-differences analysis (post-ban lift) with memory optimization
    
    Parameters:
    -----------
    treatment_period : str
        'two_weeks' : First two weeks after ban lift (April 29 - May 12, 2023)
        'four_weekdays' : First 4 weekdays after ban lift (May 1-4, 2023, since April 29 was Saturday)
    ban_period_weeks : int
        Number of weeks during the ban to include as pre-period (default: 4)
        This uses the last N weeks of the ban period (April 1-28, 2023) as the pre-period
    """
    print(f"Preparing DiD variables for treatment period: {treatment_period}...")
    print(f"Using {ban_period_weeks} weeks during ban as pre-period")
    
    # Optimize datatypes
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    
    # OPTIMIZATION: Use float32 to save 50% RAM on numbers
    cols = ['additions', 'deletions', 'changes']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('float32')
        
    df = df.dropna(subset=['additions', 'username', 'country', 'event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date
    
    # Define key dates
    ban_end = pd.Timestamp('2023-04-28').date()  # Ban lifted on April 28
    lift_date = pd.Timestamp('2023-04-29').date()  # First day after ban lift
    
    # Calculate ban period to use (last N weeks of ban)
    ban_period_start = ban_end - timedelta(weeks=ban_period_weeks) + timedelta(days=1)
    print(f"Pre-period (during ban): {ban_period_start} to {ban_end} ({ban_period_weeks} weeks during ban)")
    
    # Filter Logic
    if treatment_period == 'two_weeks':
        # First two weeks after ban lift: April 29 - May 12, 2023
        post_lift_end = lift_date + timedelta(weeks=2)
        mask = (df['date'] >= ban_period_start) & (df['date'] < post_lift_end)
        print(f"Filtered to {ban_period_weeks} weeks during ban and 2 weeks after lift")
    elif treatment_period == 'four_weekdays':
        # First 4 weekdays after ban lift: May 1-4, 2023
        may_5th = pd.Timestamp('2023-05-05').date()
        mask = (df['date'] >= ban_period_start) & (df['date'] < may_5th)
        print(f"Filtered to {ban_period_weeks} weeks during ban and first 4 weekdays after lift")
    
    df = df[mask].copy()
    
    # Aggregate
    print("Aggregating commits by user and date...")
    daily_user_stats = df.groupby(['username', 'country', 'date']).agg({
        'additions': 'sum',
        'deletions': 'sum', 
        'changes': 'sum',
        'commit_sha': 'count'
    }).reset_index()
    
    daily_user_stats.rename(columns={'commit_sha': 'commits_count'}, inplace=True)
    
    # Variables
    daily_user_stats['treatment'] = (daily_user_stats['country'].str.lower() == 'italy').astype(int)
    daily_user_stats['date_dt'] = pd.to_datetime(daily_user_stats['date'])
    
    # Post-lift logic
    if treatment_period == 'two_weeks':
        # Post-lift: April 29 - May 12, 2023
        post_lift_end = lift_date + timedelta(weeks=2)
        daily_user_stats['post_lift'] = ((daily_user_stats['date'] >= lift_date) & 
                                         (daily_user_stats['date'] < post_lift_end)).astype(int)
    elif treatment_period == 'four_weekdays':
        # Post-lift: Only May 1-4, 2023 (first 4 weekdays after lift)
        may_1st = pd.Timestamp('2023-05-01').date()
        may_5th = pd.Timestamp('2023-05-05').date()
        daily_user_stats['is_weekday'] = (daily_user_stats['date_dt'].dt.dayofweek < 5).astype(int)
        daily_user_stats['post_lift'] = ((daily_user_stats['date'] >= may_1st) & 
                                        (daily_user_stats['date'] < may_5th) &
                                        (daily_user_stats['is_weekday'] == 1)).astype(int)
    
    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_lift']
    
    # Time Trend
    min_date = daily_user_stats['date_dt'].min()
    daily_user_stats['days_since_start'] = (daily_user_stats['date_dt'] - min_date).dt.days
    
    # Create interaction term explicitly for linearmodels
    daily_user_stats['treatment_time_trend'] = daily_user_stats['treatment'] * daily_user_stats['days_since_start']
    
    # Log outcomes
    for col in ['additions', 'deletions', 'changes', 'commits_count']:
        daily_user_stats[f'log_{col}'] = np.log1p(daily_user_stats[col])

    # Categoricals for efficiency
    daily_user_stats['username'] = daily_user_stats['username'].astype('category')
    daily_user_stats['date'] = daily_user_stats['date'].astype('category')
    
    print(f"Aggregated to {len(daily_user_stats)} user-day observations")
    
    return daily_user_stats

def run_fast_regression(df, outcome_var, outcome_name):
    """
    OPTIMIZATION: Uses linearmodels AbsorbingLS for speed.
    Absorbs Fixed Effects instead of creating dummy columns.
    """
    print(f"\nRunning Fast Regression for: {outcome_name}")
    
    if LINEARMODELS_AVAILABLE:
        # Prepare data for Linearmodels
        # We need to separate the "absorbed" effects (FE) from the regressors
        
        # 1. Define Fixed Effects (Absorb)
        # 'date' + 'username' is standard TWFE.
        absorb_cols = ['username', 'date'] 
        
        # 2. Define Regressors (X)
        # We need the interaction (DiD estimator) and the group-specific trend
        exog_vars = ['treatment_post', 'treatment_time_trend']
        
        # 3. Define Dependent (Y)
        y = df[outcome_var]
        X = df[exog_vars]
        
        print(f"Absorbing {df['username'].nunique()} users and {df['date'].nunique()} dates...")
        
        try:
            # clusters should be series or dataframe
            model = AbsorbingLS(y, X, absorb=df[absorb_cols])
            
            # Clustered standard errors
            results = model.fit(cov_type='clustered', clusters=df[['username']])
            
            return results
            
        except Exception as e:
            print(f"Linearmodels failed: {e}")
            raise
    else:
        # Fallback to the slow original method if library missing
        formula = f"{outcome_var} ~ treatment_post + treatment_time_trend + C(username) + C(date)"
        return ols(formula, data=df).fit().get_robustcov_results(cov_type='cluster', groups=df['username'])

def export_results_latex(results_dict, output_path):
    """Simple custom LaTeX table generator for linearmodels results"""
    print(f"Saving LaTeX table to {output_path}")
    
    with open(output_path, 'w') as f:
        f.write(r"\begin{tabular}{l|cc|cc}" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r" & \multicolumn{2}{c|}{\textbf{Log Additions}} & \multicolumn{2}{c}{\textbf{Log Deletions}} \\" + "\n")
        f.write(r" & 2 Weeks & 4 Weekdays & 2 Weeks & 4 Weekdays \\" + "\n")
        f.write(r"\hline" + "\n")
        
        # Row: Treatment x Post-Lift
        row_coef = "Italy $\\times$ Post-Lift"
        row_se = ""
        
        # Iterate through the 4 configurations
        configs = [('log_additions', 'two_weeks'), ('log_additions', 'four_weekdays'),
                   ('log_deletions', 'two_weeks'), ('log_deletions', 'four_weekdays')]
        
        for outcome, period in configs:
            res = results_dict.get((outcome, period))
            if res:
                val = res.params['treatment_post']
                se = res.std_errors['treatment_post']
                pval = res.pvalues['treatment_post']
                
                stars = ""
                if pval < 0.01: stars = "***"
                elif pval < 0.05: stars = "**"
                elif pval < 0.1: stars = "*"
                
                # Only add superscript if there are stars
                if stars:
                    row_coef += f" & {val:.4f}^{{{stars}}}"
                else:
                    row_coef += f" & {val:.4f}"
                row_se += f" & ({se:.4f})"
            else:
                row_coef += " & -"
                row_se += " & -"
                
        f.write(row_coef + r" \\" + "\n")
        f.write(row_se + r" \\" + "\n")
        
        # Note: Group-specific time trends are included in the model but not shown in table
        # (They are control variables, not of primary interest)
        
        f.write(r"\hline" + "\n")
        
        # Stats rows
        row_obs = "Observations"
        row_r2 = "$R^2$"
        
        for outcome, period in configs:
            res = results_dict.get((outcome, period))
            if res:
                # Format numbers without commas to avoid LaTeX issues
                row_obs += f" & {int(res.nobs)}"
                row_r2 += f" & {res.rsquared:.3f}"
            else:
                row_obs += " & -"
                row_r2 += " & -"
                
        f.write(row_obs + r" \\" + "\n")
        f.write(row_r2 + r" \\" + "\n")
        f.write(r"User FE & Yes & Yes & Yes & Yes \\" + "\n")
        f.write(r"Date FE & Yes & Yes & Yes & Yes \\" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r"\end{tabular}" + "\n")

def main():
    try:
        # 1. Load Data (Optimized SQL)
        df_raw = load_and_prepare_data()
        
        models_dict = {}
        treatment_periods = ['two_weeks', 'four_weekdays']
        outcomes = {'log_additions': 'Log Additions', 'log_deletions': 'Log Deletions'}
        
        print("\n" + "="*80)
        print("RUNNING DIFFERENCE-IN-DIFFERENCES ANALYSES: POST-BAN LIFT")
        print("="*80)
        
        # 2. Run Analysis
        for period in treatment_periods:
            print(f"\n{'='*80}")
            print(f"TREATMENT PERIOD: {period.upper().replace('_', ' ')}")
            print(f"{'='*80}")
            
            # Prepare specific slice (cheap operation now that df_raw is smaller)
            df = prepare_did_variables(df_raw.copy(), treatment_period=period, ban_period_weeks=4)
            
            for outcome_var, outcome_name in outcomes.items():
                print(f"\n{'-'*60}")
                print(f"OUTCOME: {outcome_name}")
                print(f"{'-'*60}")
                
                # Run Optimized Regression
                res = run_fast_regression(df, outcome_var, outcome_name)
                models_dict[(outcome_var, period)] = res
                
                # Print Quick Summary
                print(res.summary)
        
        # 3. Export Table
        output_path = "../final report/parts/ban_lift_regression_table.tex"
        export_results_latex(models_dict, output_path)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()

