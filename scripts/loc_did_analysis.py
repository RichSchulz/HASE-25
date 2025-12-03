"""
Difference-in-Differences Analysis on LOC Added, Deleted and Total Changes.
Analyzes data 4 weeks prior to the ban and then for both a 2 week period into the ban
and a 4 work-day period into the ban.
"""

import pandas as pd
import numpy as np
import sqlite3
from linearmodels.iv import AbsorbingLS
from typing import cast


def load_and_prepare_data():
    """
    Filter by date inside SQL to avoid loading massive history.
    """
    print("Loading commit data from SQLite database...")
    
    db_path = "large_data/data_commits.sqlite3"
    
    # Calculate safe date buffer for SQL (give extra weeks to be safe)
    # Ban date is 2023-04-01. We need roughly March 1st to April 30th.
    # We filter strictly later in pandas, this is just to save RAM on load.
    sql_start_date = '2023-02-15' 
    sql_end_date = '2023-05-01'

    conn = sqlite3.connect(db_path)
    
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

def prepare_did_variables(
        df: pd.DataFrame,
        pre_start: pd.Timestamp,
        pre_end: pd.Timestamp,
        treatment_start: pd.Timestamp,
        treatment_end: pd.Timestamp,
        check_weekday: bool
):
    """
    Prepare variables for DiD analysis with memory optimization
    """

    pre_start_date = pre_start.date()
    pre_end_date = pre_end.date()
    treatment_start_date = treatment_start.date()
    treatment_end_date = treatment_end.date()
    
    # Optimize datatypes
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    
    cols = ['additions', 'deletions', 'changes']
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors='coerce')
        
    df = df.dropna(subset=['additions', 'username', 'country', 'event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date
        
    df = cast(pd.DataFrame, df[(df['date'] >= pre_start_date) & (df['date'] < treatment_end_date)]).copy()
    
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
    
    if check_weekday:
        daily_user_stats['is_weekday'] = (daily_user_stats['date_dt'].dt.dayofweek < 5).astype(int)
        daily_user_stats['post_treatment'] = (
            (daily_user_stats['date'] >= treatment_start_date) & 
            (daily_user_stats['date'] < treatment_end_date) &
            (daily_user_stats['is_weekday'] == 1)
        ).astype(int)
    else:
        daily_user_stats['post_treatment'] = (
            (daily_user_stats['date'] >= treatment_start_date) & 
            (daily_user_stats['date'] < treatment_end_date)
        ).astype(int)

    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_treatment']
    
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
    
    return daily_user_stats

def run_regression(df, outcome_var, outcome_name):
    """
    Run regression with absorbed fixed effects for user and date
    """
    print(f"\nRunning Regression for: {outcome_name}")
    
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

def export_results_latex(results_dict, output_path):
    """Simple custom LaTeX table generator for linearmodels results"""
    print(f"Saving LaTeX table to {output_path}")
    
    with open(output_path, 'w') as f:
        f.write(r"\begin{tabular}{l|cc|cc}" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r" & \multicolumn{2}{c|}{\textbf{Log Additions}} & \multicolumn{2}{c}{\textbf{Log Deletions}} \\" + "\n")
        f.write(r" & 2 Weeks & 4 Weekdays & 2 Weeks & 4 Weekdays \\" + "\n")
        f.write(r"\hline" + "\n")
        
        # Row: Treatment x Post
        row_coef = "Italy $\\times$ Post"
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
                
                row_coef += f" & ${val:.4f}^{{{stars}}}$"
                row_se += f" & $({se:.4f})$"
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
                row_obs += f" & ${int(res.nobs):,}$"
                row_r2 += f" & ${res.rsquared:.3f}$"
            else:
                row_obs += " & -"
                row_r2 += " & -"
                
        f.write(row_obs + r" \\" + "\n")
        f.write(row_r2 + r" \\" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r"\end{tabular}" + "\n")

def main():
    # Load Data
    df_raw = load_and_prepare_data()
    
    models_dict = {}
    treatment_periods = ['two_weeks', 'four_weekdays']
    outcomes = {'log_additions': 'Log Additions', 'log_deletions': 'Log Deletions'}
    
    # Run Analysis
    for period in treatment_periods:
        print(f"\n{'='*80}")
        print(f"TREATMENT PERIOD: {period.upper().replace('_', ' ')}")
        print(f"{'='*80}")

        # Prepare specific slice (cheap operation now that df_raw is smaller)
        df = prepare_did_variables(
            df_raw.copy(),
            pre_start=cast(pd.Timestamp, pd.Timestamp('2023-03-04')),
            pre_end=cast(pd.Timestamp, pd.Timestamp('2023-03-31')),
            treatment_start=cast(pd.Timestamp, pd.Timestamp('2023-04-01') if period == 'two_weeks' else pd.Timestamp('2023-04-03')),
            treatment_end=cast(pd.Timestamp, pd.Timestamp('2023-04-15') if period == 'two_weeks' else pd.Timestamp('2023-04-07')),
            check_weekday=True if period == 'four_weekdays' else False,
        )
        
        for outcome_var, outcome_name in outcomes.items():
            print(f"\n{'-'*60}")
            print(f"OUTCOME: {outcome_name}")
            print(f"{'-'*60}")
    
            # Run Optimized Regression
            res = run_regression(df, outcome_var, outcome_name)
            models_dict[(outcome_var, period)] = res
            
            # Print Quick Summary
            print(res.summary)
            
    # Export Table
    output_path = "../final report/parts/did_regression_table.tex"
    export_results_latex(models_dict, output_path)

    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE!")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
