#!/usr/bin/env python3
"""
Simplified Difference-in-Differences Analysis
Analyzes commit activity changes after April 1st, 2023, with Italy as treatment group
and Austria/France as control groups. Includes day-of-week controls.
"""

import pandas as pd
import numpy as np
from statsmodels.formula.api import ols
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """Load and prepare the enriched commit data for DiD analysis"""
    print("Loading enriched commit data...")
    
    # Hardcoded paths
    input_dir = "/Users/richard/University/HASE-25/scripts/data"
    countries = ['italy', 'france', 'austria']
    dfs = []
    
    for country in countries:
        file_path = f"{input_dir}/commits_sample_{country}_april_2023_enriched.csv"
        df = pd.read_csv(file_path)
        df['country'] = country
        dfs.append(df)
        print(f"Loaded {len(df)} commits from {country}")
    
    # Combine all data
    combined_df = pd.concat(dfs, ignore_index=True)
    print(f"Total commits loaded: {len(combined_df)}")
    
    return combined_df

def prepare_did_variables(df, seven_days_only=False):
    """Prepare variables for difference-in-differences analysis"""
    print("Preparing DiD variables...")
    
    # Convert event_timestamp to datetime
    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    
    # Handle missing API data
    df['api_additions'] = pd.to_numeric(df['api_additions'], errors='coerce')
    df['api_deletions'] = pd.to_numeric(df['api_deletions'], errors='coerce')
    df['api_changes'] = pd.to_numeric(df['api_changes'], errors='coerce')
    
    # Filter out rows with missing API data
    initial_count = len(df)
    df = df.dropna(subset=['api_additions', 'api_deletions', 'api_changes'])
    final_count = len(df)
    print(f"Removed {initial_count - final_count} commits with missing API data")
    print(f"Final dataset: {final_count} commits")
    
    # Create date column (without time)
    df['date'] = df['event_timestamp'].dt.date
    
    # Filter by time period if seven_days_only is True
    if seven_days_only:
        april_1st = pd.Timestamp('2023-04-01').date()
        april_8th = pd.Timestamp('2023-04-08').date()
        # Keep both before and after periods, but limit after to 7 days
        df = df[(df['date'] < april_8th)]  # Keep all data before April 8th
        print(f"Filtered to 7 days after April 1st: {len(df)} commits")
    
    # Aggregate by user and date
    print("Aggregating commits by user and date...")
    daily_user_stats = df.groupby(['username', 'country', 'date']).agg({
        'api_additions': 'sum',
        'api_deletions': 'sum', 
        'api_changes': 'sum',
        'commit_sha': 'count'  # Number of commits per user per day
    }).reset_index()
    
    daily_user_stats.rename(columns={'commit_sha': 'commits_count'}, inplace=True)
    
    print(f"Aggregated to {len(daily_user_stats)} user-day observations")
    
    # Create treatment group indicator (Italy = 1, others = 0)
    daily_user_stats['treatment'] = (daily_user_stats['country'] == 'italy').astype(int)
    
    # Create post-treatment indicator (after April 1st, 2023 = 1, before = 0)
    april_1st = pd.Timestamp('2023-04-01').date()
    daily_user_stats['post_treatment'] = (daily_user_stats['date'] >= april_1st).astype(int)
    
    # Create interaction term (treatment * post_treatment)
    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_treatment']
    
    # Add day of week controls
    daily_user_stats['date_dt'] = pd.to_datetime(daily_user_stats['date'])
    daily_user_stats['day_of_week'] = daily_user_stats['date_dt'].dt.day_name()
    
    # Create log variables
    daily_user_stats['log_additions'] = np.log1p(daily_user_stats['api_additions'])
    daily_user_stats['log_deletions'] = np.log1p(daily_user_stats['api_deletions'])
    daily_user_stats['log_changes'] = np.log1p(daily_user_stats['api_changes'])
    
    return daily_user_stats

def difference_in_differences_analysis(df):
    """Perform difference-in-differences regression analysis"""
    print("\n" + "="*80)
    print("DIFFERENCE-IN-DIFFERENCES REGRESSION ANALYSIS")
    print("(Analyzing total lines per user per day)")
    print("="*80)
    
    # Define log outcome variables
    outcomes = {
        'log_additions': 'Log(Lines Added + 1)',
        'log_deletions': 'Log(Lines Deleted + 1)',
        'log_changes': 'Log(Total Changes + 1)'
    }
    
    for outcome_var, outcome_name in outcomes.items():
        print(f"\n{'-'*60}")
        print(f"OUTCOME: {outcome_name}")
        print(f"{'-'*60}")
        
        # DiD regression with day-of-week controls
        formula = f"{outcome_var} ~ treatment + post_treatment + treatment_post + C(day_of_week)"
        model = ols(formula, data=df).fit()
        
        # Extract key coefficients
        coef_treatment = model.params['treatment']
        coef_post = model.params['post_treatment'] 
        coef_interaction = model.params['treatment_post']
        p_val = model.pvalues['treatment_post']
        
        # Significance indicators
        if p_val < 0.001:
            significance = "***"
        elif p_val < 0.01:
            significance = "**"
        elif p_val < 0.05:
            significance = "*"
        else:
            significance = ""
        
        print(f"Treatment effect (Italy vs Control): {coef_treatment:.4f}")
        print(f"Time effect (After vs Before): {coef_post:.4f}")
        print(f"DiD Effect (treatment_post): {coef_interaction:.4f} {significance}")
        print(f"P-value for DiD effect: {p_val:.4f}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Simplified DiD Analysis of Commit Activity")
    parser.add_argument("--seven-days-only", action="store_true",
                       help="Analyze only 7 days after April 1st instead of 14 days")
    
    args = parser.parse_args()
    
    try:
        # Load and prepare data
        df = load_and_prepare_data()
        df = prepare_did_variables(df, seven_days_only=args.seven_days_only)
        
        # Perform DiD analysis
        difference_in_differences_analysis(df)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()
