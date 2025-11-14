#!/usr/bin/env python3
"""
Difference-in-Differences Analysis: Post-Ban Lift
Analyzes commit activity changes after the ChatGPT ban was lifted (April 28, 2023),
comparing activity during the ban with activity after the ban was lifted.
Italy as treatment group and Austria/France as control groups.
Includes day-of-week controls and clustered standard errors by user.
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
try:
    from stargazer.stargazer import Stargazer
    # Fix stargazer bug: statsmodels translator is missing pandas import
    try:
        import stargazer.translators.statsmodels as stargazer_statsmodels
        if not hasattr(stargazer_statsmodels, 'pd'):
            stargazer_statsmodels.pd = pd
    except (ImportError, AttributeError):
        pass  # If we can't patch it, try to continue anyway
    STARGAZER_AVAILABLE = True
except ImportError:
    STARGAZER_AVAILABLE = False
    print("Warning: stargazer not available. Install with: pip install stargazer")
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

def prepare_did_variables(df, treatment_period='two_weeks', ban_period_weeks=4):
    """
    Prepare variables for difference-in-differences analysis (post-ban lift)
    
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
    
    # Define key dates
    ban_start = pd.Timestamp('2023-04-01').date()
    ban_end = pd.Timestamp('2023-04-28').date()  # Ban lifted on April 28
    lift_date = pd.Timestamp('2023-04-29').date()  # First day after ban lift
    
    # Calculate ban period to use (last N weeks of ban)
    ban_period_start = ban_end - timedelta(weeks=ban_period_weeks) + timedelta(days=1)
    print(f"Pre-period (during ban): {ban_period_start} to {ban_end} ({ban_period_weeks} weeks during ban)")
    
    # Filter by treatment period
    if treatment_period == 'two_weeks':
        # First two weeks after ban lift: April 29 - May 12, 2023
        post_lift_end = lift_date + timedelta(weeks=2)
        # Keep data from ban period start to end of post-lift period
        df = df[(df['date'] >= ban_period_start) & (df['date'] < post_lift_end)]
        print(f"Filtered to {ban_period_weeks} weeks during ban and 2 weeks after lift: {len(df)} commits")
        print(f"Date range: {ban_period_start} to {post_lift_end - timedelta(days=1)}")
    elif treatment_period == 'four_weekdays':
        # First 4 weekdays after ban lift: May 1-4, 2023 (April 29 was Saturday, April 30 was Sunday)
        may_5th = pd.Timestamp('2023-05-05').date()
        # Keep data from ban period start to end of post-lift period
        df = df[(df['date'] >= ban_period_start) & (df['date'] < may_5th)]
        print(f"Filtered to {ban_period_weeks} weeks during ban and first 4 weekdays after lift: {len(df)} commits")
        print(f"Date range: {ban_period_start} to {may_5th - timedelta(days=1)}")
    
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
    
    # Add date datetime for additional controls
    daily_user_stats['date_dt'] = pd.to_datetime(daily_user_stats['date'])
    
    # Create post-lift indicator based on treatment period
    if treatment_period == 'two_weeks':
        # Post-lift: April 29 - May 12, 2023
        post_lift_end = lift_date + timedelta(weeks=2)
        daily_user_stats['post_lift'] = ((daily_user_stats['date'] >= lift_date) & 
                                         (daily_user_stats['date'] < post_lift_end)).astype(int)
    elif treatment_period == 'four_weekdays':
        # Post-lift: Only May 1-4, 2023 (first 4 weekdays after lift)
        may_1st = pd.Timestamp('2023-05-01').date()
        may_5th = pd.Timestamp('2023-05-05').date()
        # Only count weekdays (Monday=0, Sunday=6)
        daily_user_stats['is_weekday'] = (daily_user_stats['date_dt'].dt.dayofweek < 5).astype(int)
        daily_user_stats['post_lift'] = ((daily_user_stats['date'] >= may_1st) & 
                                        (daily_user_stats['date'] < may_5th) &
                                        (daily_user_stats['is_weekday'] == 1)).astype(int)
    
    # Create interaction term (treatment * post_lift)
    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_lift']
    
    # Add day of week controls
    daily_user_stats['day_of_week'] = daily_user_stats['date_dt'].dt.day_name()
    
    # Ensure treatment and post_lift are regular int types (not nullable)
    daily_user_stats['treatment'] = daily_user_stats['treatment'].astype(int)
    daily_user_stats['post_lift'] = daily_user_stats['post_lift'].astype(int)
    daily_user_stats['treatment_post'] = daily_user_stats['treatment_post'].astype(int)
    
    # Convert all numeric columns to standard numpy types to avoid nullable dtype issues
    for col in ['additions', 'deletions', 'changes', 'commits_count']:
        if col in daily_user_stats.columns:
            daily_user_stats[col] = daily_user_stats[col].astype(float)
    
    return daily_user_stats

def run_did_regression(df, outcome_var, outcome_name):
    """
    Run a single DiD regression and return the model with clustered standard errors
    
    Parameters:
    -----------
    df : DataFrame
        Prepared data with DiD variables
    outcome_var : str
        Name of outcome variable (e.g., 'additions', 'deletions')
    outcome_name : str
        Human-readable name for the outcome
    
    Returns:
    --------
    model_clustered : RegressionResults
        Fitted model with clustered standard errors
    """
    print(f"\nRunning regression for: {outcome_name}")
    
    # DiD regression with day-of-week controls
    formula = f"{outcome_var} ~ treatment + post_lift + treatment_post + C(day_of_week)"
    
    try:
        model = ols(formula, data=df).fit()
        
        # Check for perfect multicollinearity
        if hasattr(model, 'condition_number'):
            cond_num = model.condition_number
            if cond_num > 1e10:
                print(f"Warning: High condition number ({cond_num:.2e}), trying simpler model...")
                formula = f"{outcome_var} ~ treatment + post_lift + treatment_post"
                model = ols(formula, data=df).fit()
    except Exception as e:
        print(f"Error fitting model: {e}")
        print("Trying simplest possible specification...")
        formula = f"{outcome_var} ~ treatment + post_lift + treatment_post"
        model = ols(formula, data=df).fit()
    
    # Get clustered standard errors by user
    user_groups = df['username'].values
    unique_users = pd.unique(user_groups)
    user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
    group_indices = np.array([user_to_idx[user] for user in user_groups])
    
    try:
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
    
    return model_clustered

def create_latex_table(models_dict, output_path):
    """
    Create a LaTeX table using stargazer with all regression models
    
    Parameters:
    -----------
    models_dict : dict
        Dictionary with keys like (outcome, period) and values as model objects
        e.g., {('additions', 'two_weeks'): model1, ('additions', 'four_weekdays'): model2, ...}
    output_path : str
        Path to save the LaTeX table
    """
    if not STARGAZER_AVAILABLE:
        print("Error: stargazer is not available. Cannot create LaTeX table.")
        print("Install with: pip install stargazer")
        return
    
    print("\n" + "="*80)
    print("CREATING LATEX TABLE")
    print("="*80)
    
    # Fix stargazer bug: ensure pandas is available in the statsmodels translator
    try:
        import stargazer.translators.statsmodels as stargazer_statsmodels
        if not hasattr(stargazer_statsmodels, 'pd'):
            stargazer_statsmodels.pd = pd
    except (ImportError, AttributeError):
        pass  # If we can't patch it, try to continue anyway
    
    # Organize models by outcome, then by period
    model_list = []
    column_labels = []
    
    for outcome in ['additions', 'deletions']:
        for period in ['two_weeks', 'four_weekdays']:
            key = (outcome, period)
            if key in models_dict:
                model_list.append(models_dict[key])
                # Create column label
                outcome_label = 'LOC Added' if outcome == 'additions' else 'LOC Deleted'
                period_label = '2 Weeks' if period == 'two_weeks' else '4 Weekdays'
                column_labels.append(f"{outcome_label} ({period_label})")
    
    if len(model_list) == 0:
        print("Error: No models found to include in table")
        return
    
    # Create stargazer object
    stargazer = Stargazer(model_list)
    
    # Set covariate order
    stargazer.covariate_order(['treatment', 'post_lift', 'treatment_post', 'Intercept'])
    
    # Rename covariates
    stargazer.rename_covariates({
        'treatment': 'Italy',
        'post_lift': 'Post-Lift',
        'treatment_post': 'Italy × Post-Lift',
        'Intercept': 'Constant'
    })
    
    # Configure table
    stargazer.show_model_numbers(False)
    stargazer.show_degrees_of_freedom(True)
    stargazer.custom_columns(column_labels)
    
    # Add title
    stargazer.title("Difference-in-Differences Estimates: Lines of Code Changes After ChatGPT Ban Lift")
    
    # Render LaTeX
    latex_table = stargazer.render_latex()
    
    # Save to file
    with open(output_path, 'w') as f:
        f.write(latex_table)
    
    print(f"\nLaTeX table saved to: {output_path}")
    print("\nLaTeX Table Preview:")
    print("="*80)
    print(latex_table[:500] + "..." if len(latex_table) > 500 else latex_table)
    print("="*80)

def main():
    try:
        # Load data
        df_raw = load_and_prepare_data()
        
        # Dictionary to store all models for LaTeX table
        models_dict = {}
        
        # Run analyses for both treatment periods and both outcomes
        treatment_periods = ['two_weeks', 'four_weekdays']
        outcomes = {
            'additions': 'Lines of Code Added per Developer per Day',
            'deletions': 'Lines of Code Deleted per Developer per Day'
        }
        
        print("\n" + "="*80)
        print("RUNNING DIFFERENCE-IN-DIFFERENCES ANALYSES: POST-BAN LIFT")
        print("="*80)
        
        for period in treatment_periods:
            print(f"\n{'='*80}")
            print(f"TREATMENT PERIOD: {period.upper().replace('_', ' ')}")
            print(f"{'='*80}")
            
            # Prepare data for this treatment period
            # Using 4 weeks during ban as pre-period
            df = prepare_did_variables(df_raw.copy(), treatment_period=period, ban_period_weeks=4)
            
            # Run regressions for each outcome
            for outcome_var, outcome_name in outcomes.items():
                print(f"\n{'-'*60}")
                print(f"OUTCOME: {outcome_name}")
                print(f"{'-'*60}")
                
                # Run regression
                model = run_did_regression(df, outcome_var, outcome_name)
                
                # Store model for LaTeX table
                models_dict[(outcome_var, period)] = model
                
                # Print key results
                if isinstance(model.params, pd.Series):
                    params_series = model.params
                else:
                    param_names = model.model.exog_names if hasattr(model.model, 'exog_names') else list(range(len(model.params)))
                    params_series = pd.Series(model.params, index=param_names)
                
                if isinstance(model.bse, pd.Series):
                    bse_series = model.bse
                else:
                    bse_series = pd.Series(model.bse, index=params_series.index)
                
                if isinstance(model.pvalues, pd.Series):
                    pvalues_series = model.pvalues
                else:
                    pvalues_series = pd.Series(model.pvalues, index=params_series.index)
                
                coef_interaction = params_series.get('treatment_post', np.nan)
                se_interaction = bse_series.get('treatment_post', np.nan)
                p_val_interaction = pvalues_series.get('treatment_post', np.nan)
                
                # Significance indicators
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
                
                print(f"DiD Effect (Italy × Post-Lift): {coef_interaction:.4f} (SE: {se_interaction:.4f}) {significance}")
                print(f"P-value: {p_val_interaction:.4f}")
                print(f"Observations: {len(df)}")
                print(f"Number of clusters (users): {df['username'].nunique()}")
        
        # Create LaTeX table
        output_path = "/Users/richard/University/HASE-25/final report/parts/ban_lift_regression_table.tex"
        create_latex_table(models_dict, output_path)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()

