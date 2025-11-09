#!/usr/bin/env python3
"""
Simplified Difference-in-Differences Analysis
Analyzes commit activity changes after April 1st, 2023, with Italy as treatment group
and Austria/France as control groups. Includes day-of-week controls, time trends,
and clustered standard errors by user.
"""

import pandas as pd
import numpy as np
import sqlite3
from statsmodels.formula.api import ols
from scipy import stats as scipy_stats
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import timedelta
import warnings
warnings.filterwarnings('ignore')

def load_and_prepare_data():
    """Load and prepare commit data from SQLite database for DiD analysis"""
    print("Loading commit data from SQLite database...")
    
    # Hardcoded path to SQLite database
    db_path = "/Users/richard/University/HASE-25/scripts/data/data_commits.sqlite3"
    
    # Connect to database and load data
    conn = sqlite3.connect(db_path)
    
    # Load all commit data
    query = """
    SELECT 
        country,
        repository_name,
        username,
        commit_sha,
        commit_message,
        push_event_timestamp,
        additions,
        deletions,
        changes
    FROM commit_data
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Total commits loaded: {len(df)}")
    print(f"Countries: {df['country'].unique()}")
    print(f"Unique users: {df['username'].nunique()}")
    
    return df

def prepare_did_variables(df, seven_days_only=False):
    """Prepare variables for difference-in-differences analysis"""
    print("Preparing DiD variables...")
    
    # Convert push_event_timestamp to datetime
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    
    # Handle missing data - convert to numeric
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce')
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce')
    df['changes'] = pd.to_numeric(df['changes'], errors='coerce')
    
    # Filter out rows with missing data
    initial_count = len(df)
    df = df.dropna(subset=['additions', 'deletions', 'changes', 'username', 'country', 'event_timestamp'])
    final_count = len(df)
    print(f"Removed {initial_count - final_count} commits with missing data")
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
        'additions': 'sum',
        'deletions': 'sum', 
        'changes': 'sum',
        'commit_sha': 'count'  # Number of commits per user per day
    }).reset_index()
    
    daily_user_stats.rename(columns={'commit_sha': 'commits_count'}, inplace=True)
    
    print(f"Aggregated to {len(daily_user_stats)} user-day observations")
    
    # Create treatment group indicator (Italy = 1, others = 0)
    daily_user_stats['treatment'] = (daily_user_stats['country'].str.lower() == 'italy').astype(int)
    
    # Create post-treatment indicator (after April 1st, 2023 = 1, before = 0)
    april_1st = pd.Timestamp('2023-04-01').date()
    daily_user_stats['post_treatment'] = (daily_user_stats['date'] >= april_1st).astype(int)
    
    # Create interaction term (treatment * post_treatment)
    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_treatment']
    
    # Add date datetime for additional controls
    daily_user_stats['date_dt'] = pd.to_datetime(daily_user_stats['date'])
    
    # Add day of week controls
    daily_user_stats['day_of_week'] = daily_user_stats['date_dt'].dt.day_name()
    
    # Add week of year controls
    daily_user_stats['week_of_year'] = daily_user_stats['date_dt'].dt.isocalendar()['week'].astype(int)
    
    # Add linear time trend (days since start of data)
    min_date = daily_user_stats['date_dt'].min()
    daily_user_stats['days_since_start'] = (daily_user_stats['date_dt'] - min_date).dt.days.astype(int)
    
    # Add month controls
    daily_user_stats['month'] = daily_user_stats['date_dt'].dt.month.astype(int)
    
    # Ensure treatment and post_treatment are regular int types (not nullable)
    daily_user_stats['treatment'] = daily_user_stats['treatment'].astype(int)
    daily_user_stats['post_treatment'] = daily_user_stats['post_treatment'].astype(int)
    daily_user_stats['treatment_post'] = daily_user_stats['treatment_post'].astype(int)
    
    # Create log variables
    daily_user_stats['log_additions'] = np.log1p(daily_user_stats['additions'])
    daily_user_stats['log_deletions'] = np.log1p(daily_user_stats['deletions'])
    daily_user_stats['log_changes'] = np.log1p(daily_user_stats['changes'])
    daily_user_stats['log_commits'] = np.log1p(daily_user_stats['commits_count'])
    
    # Convert all numeric columns to standard numpy types to avoid nullable dtype issues
    for col in ['additions', 'deletions', 'changes', 'commits_count']:
        if col in daily_user_stats.columns:
            daily_user_stats[col] = daily_user_stats[col].astype(float)
    
    return daily_user_stats

def difference_in_differences_analysis(df):
    """Perform difference-in-differences regression analysis with clustered standard errors"""
    print("\n" + "="*80)
    print("DIFFERENCE-IN-DIFFERENCES REGRESSION ANALYSIS")
    print("(Analyzing commits and code changes per user per day)")
    print("(Standard errors clustered by user)")
    print("="*80)
    
    # Define log outcome variables
    outcomes = {
        'log_commits': 'Log(Commits per Day + 1)',
        'log_additions': 'Log(Lines Added + 1)',
        'log_deletions': 'Log(Lines Deleted + 1)',
        'log_changes': 'Log(Total Changes + 1)'
    }
    
    for outcome_var, outcome_name in outcomes.items():
        print(f"\n{'-'*60}")
        print(f"OUTCOME: {outcome_name}")
        print(f"{'-'*60}")
        
        # DiD regression - try simplest specification first
        # The issue is likely that week_of_year and month are highly correlated with post_treatment
        # Start with basic DiD specification
        formula = f"{outcome_var} ~ treatment + post_treatment + treatment_post + C(day_of_week)"
        
        try:
            model = ols(formula, data=df).fit()
            
            # Check for perfect multicollinearity or singular matrix
            if hasattr(model, 'condition_number'):
                cond_num = model.condition_number
                if cond_num > 1e10:
                    print(f"Warning: High condition number ({cond_num:.2e}) suggests multicollinearity")
                    print("Trying simpler model without day_of_week controls...")
                    # Try even simpler model
                    formula = f"{outcome_var} ~ treatment + post_treatment + treatment_post"
                    model = ols(formula, data=df).fit()
                    if hasattr(model, 'condition_number'):
                        cond_num = model.condition_number
                        if cond_num > 1e10:
                            print(f"Warning: Still high condition number ({cond_num:.2e})")
                            print("This may indicate issues with the data structure")
                        else:
                            print(f"Simpler model has acceptable condition number ({cond_num:.2e})")
        except Exception as e:
            print(f"Error fitting model: {e}")
            print("Trying simplest possible specification...")
            formula = f"{outcome_var} ~ treatment + post_treatment + treatment_post"
            model = ols(formula, data=df).fit()
        
        # Get clustered standard errors by user
        # Create user group indices for clustering
        user_groups = df['username'].values
        unique_users = pd.unique(user_groups)
        user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
        group_indices = np.array([user_to_idx[user] for user in user_groups])
        
        # Get clustered standard errors using get_robustcov_results
        # First try regular robust standard errors to see if model works
        print("Computing standard errors...")
        try:
            # Try clustered standard errors
            model_clustered = model.get_robustcov_results(cov_type='cluster', groups=group_indices)
            print("Using clustered standard errors (by user)")
        except Exception as e:
            print(f"Warning: Clustered standard errors failed: {e}")
            print("Falling back to robust standard errors (HC1)")
            try:
                model_clustered = model.get_robustcov_results(cov_type='HC1')
            except Exception as e2:
                print(f"Warning: HC1 standard errors also failed: {e2}")
                print("Using default standard errors (may be unreliable)")
                model_clustered = model
        
        # Extract key coefficients - coefficients don't change with clustering, only standard errors do
        # Get parameter names - use params.index if it's a Series, otherwise try to get from model
        if isinstance(model.params, pd.Series):
            param_names = model.params.index
            params_series = model.params
        else:
            # If params is not a Series, try to get names from model
            try:
                param_names = model.model.exog_names
            except AttributeError:
                try:
                    param_names = model.model.data.param_names
                except AttributeError:
                    # Fallback: use numeric indices
                    param_names = list(range(len(model.params)))
            params_series = pd.Series(model.params, index=param_names)
        
        coef_treatment = params_series['treatment']
        coef_post = params_series['post_treatment'] 
        coef_interaction = params_series['treatment_post']
        
        # Get standard errors and p-values from clustered model
        # Convert to Series if needed using the same parameter names
        if isinstance(model_clustered.bse, pd.Series):
            bse_series = model_clustered.bse
        else:
            bse_series = pd.Series(model_clustered.bse, index=param_names)
        
        if isinstance(model_clustered.pvalues, pd.Series):
            pvalues_series = model_clustered.pvalues
        else:
            pvalues_series = pd.Series(model_clustered.pvalues, index=param_names)
        
        # Extract values, handling NaN cases
        se_treatment = bse_series.get('treatment', np.nan)
        se_post = bse_series.get('post_treatment', np.nan)
        se_interaction = bse_series.get('treatment_post', np.nan)
        
        p_val_treatment = pvalues_series.get('treatment', np.nan)
        p_val_post = pvalues_series.get('post_treatment', np.nan)
        p_val_interaction = pvalues_series.get('treatment_post', np.nan)
        
        # Check for NaN values and provide diagnostics
        if np.isnan(se_treatment) or np.isnan(se_post) or np.isnan(se_interaction):
            print("Warning: Some standard errors are NaN. This may indicate:")
            print("  - Perfect multicollinearity in the model")
            print("  - Issues with the clustered covariance matrix")
            print("  - Insufficient variation in some variables")
            print("\nTrying to diagnose...")
            
            # Check if covariance matrix has issues
            try:
                cov_matrix = model_clustered.cov_params()
                if cov_matrix is not None:
                    # Check for NaN or Inf in covariance matrix
                    nan_count = np.isnan(cov_matrix).sum().sum()
                    inf_count = np.isinf(cov_matrix).sum().sum()
                    if nan_count > 0:
                        print(f"  - Found {nan_count} NaN values in covariance matrix")
                    if inf_count > 0:
                        print(f"  - Found {inf_count} Inf values in covariance matrix")
                    
                    # Check diagonal elements (variances) for the key variables
                    key_vars = ['treatment', 'post_treatment', 'treatment_post']
                    for var in key_vars:
                        if var in cov_matrix.index:
                            var_val = cov_matrix.loc[var, var]
                            if np.isnan(var_val) or np.isinf(var_val):
                                print(f"  - Variance for '{var}' is {var_val}")
            except Exception as e:
                print(f"  - Could not check covariance matrix: {e}")
        
        # Significance indicators for interaction term
        if not np.isnan(p_val_interaction):
            if p_val_interaction < 0.001:
                significance = "***"
            elif p_val_interaction < 0.01:
                significance = "**"
            elif p_val_interaction < 0.05:
                significance = "*"
            else:
                significance = ""
        else:
            significance = ""
        
        # Format output, handling NaN values
        se_treatment_str = f"{se_treatment:.4f}" if not np.isnan(se_treatment) else "NaN"
        se_post_str = f"{se_post:.4f}" if not np.isnan(se_post) else "NaN"
        se_interaction_str = f"{se_interaction:.4f}" if not np.isnan(se_interaction) else "NaN"
        
        p_val_treatment_str = f"{p_val_treatment:.4f}" if not np.isnan(p_val_treatment) else "NaN"
        p_val_post_str = f"{p_val_post:.4f}" if not np.isnan(p_val_post) else "NaN"
        p_val_interaction_str = f"{p_val_interaction:.4f}" if not np.isnan(p_val_interaction) else "NaN"
        
        print(f"Treatment effect (Italy vs Control): {coef_treatment:.4f} (SE: {se_treatment_str}, p={p_val_treatment_str})")
        print(f"Time effect (After vs Before): {coef_post:.4f} (SE: {se_post_str}, p={p_val_post_str})")
        print(f"DiD Effect (treatment_post): {coef_interaction:.4f} (SE: {se_interaction_str}) {significance}")
        print(f"P-value for DiD effect: {p_val_interaction_str}")
        print(f"Number of clusters (users): {len(unique_users)}")
        print(f"Observations: {len(df)}")

def plot_lines_added_before_after(df):
    """Plot average lines added per developer per day for 7 days before and after the ban"""
    print("\n" + "="*80)
    print("CREATING PLOT: Average Lines Added per Developer per Day")
    print("(7 days before and 7 days after April 1st, 2023)")
    print("="*80)
    
    # Define the ban date
    ban_date = pd.Timestamp('2023-04-01').date()
    
    # Define date range: 7 days before (March 25 - March 31) and 7 days after (April 1 - April 7)
    start_date = ban_date - timedelta(days=7)
    end_date = ban_date + timedelta(days=6)  # April 7th
    
    # Filter data to this date range
    df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
    
    if len(df_filtered) == 0:
        print("Warning: No data found in the specified date range")
        return
    
    print(f"Filtered data: {len(df_filtered)} observations")
    print(f"Date range: {start_date} to {end_date}")
    
    # Calculate average lines added per developer per day
    # Group by date, treatment group, and calculate mean and std
    daily_stats = df_filtered.groupby(['date', 'treatment']).agg({
        'additions': ['mean', 'std', 'count']
    }).reset_index()
    
    # Flatten column names
    daily_stats.columns = ['date', 'treatment', 'mean_additions', 'std_additions', 'count']
    
    # Fill NaN standard deviations with 0 (occurs when only one observation per day)
    daily_stats['std_additions'] = daily_stats['std_additions'].fillna(0)
    
    # Separate treatment and control groups
    treatment_data = daily_stats[daily_stats['treatment'] == 1].copy()
    control_data = daily_stats[daily_stats['treatment'] == 0].copy()
    
    # Sort by date
    treatment_data = treatment_data.sort_values('date')
    control_data = control_data.sort_values('date')
    
    # Convert date to datetime for plotting
    treatment_data['date_dt'] = pd.to_datetime(treatment_data['date'])
    control_data['date_dt'] = pd.to_datetime(control_data['date'])
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot treatment group (Italy)
    ax.plot(treatment_data['date_dt'], treatment_data['mean_additions'], 
            marker='o', linewidth=2.5, markersize=10, label='Treatment (Italy)', color='#e74c3c')
    
    # Plot control group (Austria/France)
    ax.plot(control_data['date_dt'], control_data['mean_additions'], 
            marker='s', linewidth=2.5, markersize=10, label='Control (Austria/France)', color='#3498db')
    
    # Add vertical line at ban date
    ban_datetime = pd.Timestamp(ban_date)
    ax.axvline(x=ban_datetime, color='black', linestyle='--', linewidth=2, 
               label='Ban Date (April 1st, 2023)', alpha=0.7)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Labels and title
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Average Lines Added per Developer per Day', fontsize=12)
    ax.set_title('Average Lines Added per Developer per Day\n(7 Days Before and After April 1st, 2023)', 
                 fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Add text annotation for before/after
    ax.text(0.02, 0.98, 'Before Ban', transform=ax.transAxes, 
            fontsize=10, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    ax.text(0.52, 0.98, 'After Ban', transform=ax.transAxes, 
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/richard/University/HASE-25/scripts/data/lines_added_before_after_plot.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print("-" * 60)
    print("Treatment Group (Italy):")
    before_treatment = treatment_data[treatment_data['date'] < ban_date]
    after_treatment = treatment_data[treatment_data['date'] >= ban_date]
    if len(before_treatment) > 0:
        print(f"  Before ban: Mean = {before_treatment['mean_additions'].mean():.2f}, "
              f"Std = {before_treatment['std_additions'].mean():.2f}")
    if len(after_treatment) > 0:
        print(f"  After ban: Mean = {after_treatment['mean_additions'].mean():.2f}, "
              f"Std = {after_treatment['std_additions'].mean():.2f}")
    
    print("\nControl Group (Austria/France):")
    before_control = control_data[control_data['date'] < ban_date]
    after_control = control_data[control_data['date'] >= ban_date]
    if len(before_control) > 0:
        print(f"  Before ban: Mean = {before_control['mean_additions'].mean():.2f}, "
              f"Std = {before_control['std_additions'].mean():.2f}")
    if len(after_control) > 0:
        print(f"  After ban: Mean = {after_control['mean_additions'].mean():.2f}, "
              f"Std = {after_control['std_additions'].mean():.2f}")
    
    plt.close()

def plot_commits_before_after(df):
    """Plot average commits per developer per day for 7 days before and after the ban"""
    print("\n" + "="*80)
    print("CREATING PLOT: Average Commits per Developer per Day")
    print("(7 days before and 7 days after April 1st, 2023)")
    print("="*80)
    
    # Define the ban date
    ban_date = pd.Timestamp('2023-04-01').date()
    
    # Define date range: 7 days before (March 25 - March 31) and 7 days after (April 1 - April 7)
    start_date = ban_date - timedelta(days=7)
    end_date = ban_date + timedelta(days=6)  # April 7th
    
    # Filter data to this date range
    df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()
    
    if len(df_filtered) == 0:
        print("Warning: No data found in the specified date range")
        return
    
    print(f"Filtered data: {len(df_filtered)} observations")
    print(f"Date range: {start_date} to {end_date}")
    
    # Calculate average commits per developer per day
    # Group by date, treatment group, and calculate mean and std
    daily_stats = df_filtered.groupby(['date', 'treatment']).agg({
        'commits_count': ['mean', 'std', 'count']
    }).reset_index()
    
    # Flatten column names
    daily_stats.columns = ['date', 'treatment', 'mean_commits', 'std_commits', 'count']
    
    # Fill NaN standard deviations with 0 (occurs when only one observation per day)
    daily_stats['std_commits'] = daily_stats['std_commits'].fillna(0)
    
    # Separate treatment and control groups
    treatment_data = daily_stats[daily_stats['treatment'] == 1].copy()
    control_data = daily_stats[daily_stats['treatment'] == 0].copy()
    
    # Sort by date
    treatment_data = treatment_data.sort_values('date')
    control_data = control_data.sort_values('date')
    
    # Convert date to datetime for plotting
    treatment_data['date_dt'] = pd.to_datetime(treatment_data['date'])
    control_data['date_dt'] = pd.to_datetime(control_data['date'])
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot treatment group (Italy)
    ax.plot(treatment_data['date_dt'], treatment_data['mean_commits'], 
            marker='o', linewidth=2.5, markersize=10, label='Treatment (Italy)', color='#e74c3c')
    
    # Plot control group (Austria/France)
    ax.plot(control_data['date_dt'], control_data['mean_commits'], 
            marker='s', linewidth=2.5, markersize=10, label='Control (Austria/France)', color='#3498db')
    
    # Add vertical line at ban date
    ban_datetime = pd.Timestamp(ban_date)
    ax.axvline(x=ban_datetime, color='black', linestyle='--', linewidth=2, 
               label='Ban Date (April 1st, 2023)', alpha=0.7)
    
    # Format x-axis
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Labels and title
    ax.set_xlabel('Date', fontsize=12)
    ax.set_ylabel('Average Commits per Developer per Day', fontsize=12)
    ax.set_title('Average Commits per Developer per Day\n(7 Days Before and After April 1st, 2023)', 
                 fontsize=14, fontweight='bold')
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)
    
    # Add text annotation for before/after
    ax.text(0.02, 0.98, 'Before Ban', transform=ax.transAxes, 
            fontsize=10, verticalalignment='top', 
            bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    ax.text(0.52, 0.98, 'After Ban', transform=ax.transAxes, 
            fontsize=10, verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.5))
    
    plt.tight_layout()
    
    # Save the plot
    output_path = "/Users/richard/University/HASE-25/scripts/data/commits_before_after_plot.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    # Print summary statistics
    print("\nSummary Statistics:")
    print("-" * 60)
    print("Treatment Group (Italy):")
    before_treatment = treatment_data[treatment_data['date'] < ban_date]
    after_treatment = treatment_data[treatment_data['date'] >= ban_date]
    if len(before_treatment) > 0:
        print(f"  Before ban: Mean = {before_treatment['mean_commits'].mean():.2f}, "
              f"Std = {before_treatment['std_commits'].mean():.2f}")
    if len(after_treatment) > 0:
        print(f"  After ban: Mean = {after_treatment['mean_commits'].mean():.2f}, "
              f"Std = {after_treatment['std_commits'].mean():.2f}")
    
    print("\nControl Group (Austria/France):")
    before_control = control_data[control_data['date'] < ban_date]
    after_control = control_data[control_data['date'] >= ban_date]
    if len(before_control) > 0:
        print(f"  Before ban: Mean = {before_control['mean_commits'].mean():.2f}, "
              f"Std = {before_control['std_commits'].mean():.2f}")
    if len(after_control) > 0:
        print(f"  After ban: Mean = {after_control['mean_commits'].mean():.2f}, "
              f"Std = {after_control['std_commits'].mean():.2f}")
    
    plt.close()

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
        
        # Create plots of lines added and commits before and after ban
        plot_lines_added_before_after(df)
        plot_commits_before_after(df)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()
