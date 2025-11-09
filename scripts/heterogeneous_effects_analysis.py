#!/usr/bin/env python3
"""
Heterogeneous Effects Analysis for DiD
Analyzes how the treatment effect varies by:
1. Developer activity level (high vs low activity)
2. Repository type (personal vs organizational) - placeholder for now
3. Time since ban (immediate vs delayed effects)
"""

import pandas as pd
import numpy as np
import sqlite3
from statsmodels.formula.api import ols
from patsy import Treatment
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

def calculate_developer_activity_level(df, ban_date):
    """Calculate pre-ban activity level for each developer"""
    print("\nCalculating developer activity levels...")
    
    # Convert to datetime
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date
    
    # Define pre-ban period (30 days before ban)
    pre_ban_start = ban_date - timedelta(days=30)
    pre_ban_end = ban_date - timedelta(days=1)
    
    # Filter to pre-ban period
    pre_ban_data = df[(df['date'] >= pre_ban_start) & (df['date'] < ban_date)].copy()
    
    # Calculate daily activity per developer
    daily_user_stats = pre_ban_data.groupby(['username', 'country', 'date']).agg({
        'additions': 'sum',
        'deletions': 'sum',
        'changes': 'sum',
        'commit_sha': 'count'
    }).reset_index()
    
    daily_user_stats.rename(columns={'commit_sha': 'commits_count'}, inplace=True)
    
    # Calculate average daily activity per developer
    user_activity = daily_user_stats.groupby(['username', 'country']).agg({
        'additions': 'mean',
        'deletions': 'mean',
        'changes': 'mean',
        'commits_count': 'mean'
    }).reset_index()
    
    # Create composite activity score (weighted average)
    # Normalize each metric to 0-1 scale first
    for metric in ['additions', 'deletions', 'changes', 'commits_count']:
        max_val = user_activity[metric].max()
        if max_val > 0:
            user_activity[f'{metric}_norm'] = user_activity[metric] / max_val
        else:
            user_activity[f'{metric}_norm'] = 0
    
    # Composite score (equal weights)
    user_activity['activity_score'] = (
        user_activity['additions_norm'] * 0.3 +
        user_activity['deletions_norm'] * 0.2 +
        user_activity['changes_norm'] * 0.3 +
        user_activity['commits_count_norm'] * 0.2
    )
    
    # Classify as high/low activity (median split)
    median_activity = user_activity['activity_score'].median()
    user_activity['high_activity'] = (user_activity['activity_score'] >= median_activity).astype(int)
    
    print(f"Median activity score: {median_activity:.4f}")
    print(f"High activity developers: {user_activity['high_activity'].sum()}")
    print(f"Low activity developers: {(user_activity['high_activity'] == 0).sum()}")
    
    # Return mapping of username -> activity level
    activity_map = user_activity[['username', 'country', 'high_activity', 'activity_score']].copy()
    
    return activity_map

def load_organization_data_from_csv():
    """
    Try to load organization data from CSV sample files
    Returns a DataFrame with repository_name, username, organization_name, organization_id
    """
    import glob
    import os
    
    csv_files = [
        "/Users/richard/University/HASE-25/scripts/data/commits_sample_italy_april_2023_enriched.csv",
        "/Users/richard/University/HASE-25/scripts/data/commits_sample_austria_april_2023_enriched.csv",
        "/Users/richard/University/HASE-25/scripts/data/commits_sample_france_april_2023_enriched.csv"
    ]
    
    org_data_list = []
    
    for csv_file in csv_files:
        if os.path.exists(csv_file):
            try:
                # Read only relevant columns
                df_csv = pd.read_csv(csv_file, usecols=['repository_name', 'username', 
                                                         'organization_name', 'organization_id'],
                                     dtype=str, low_memory=False)
                org_data_list.append(df_csv)
            except Exception as e:
                print(f"Warning: Could not load {csv_file}: {e}")
    
    if org_data_list:
        org_data = pd.concat(org_data_list, ignore_index=True)
        # Remove duplicates
        org_data = org_data.drop_duplicates(subset=['repository_name', 'username'])
        print(f"Loaded organization data for {len(org_data)} repository-user pairs")
        return org_data
    else:
        print("No organization data found in CSV files")
        return None

def classify_repository_type(df, org_data=None):
    """
    Classify repositories as personal vs organizational
    
    Args:
        df: DataFrame with repository_name and username
        org_data: Optional DataFrame with organization_name from CSV files
    
    Returns:
        DataFrame with repository_name, username, is_personal_repo, organization_name
    """
    print("\nClassifying repository types...")
    
    # Start with repository name and username
    repo_type = df[['repository_name', 'username']].drop_duplicates().copy()
    
    # If organization data is available, use it
    if org_data is not None:
        # Merge organization data
        repo_type = repo_type.merge(
            org_data[['repository_name', 'username', 'organization_name', 'organization_id']],
            on=['repository_name', 'username'],
            how='left'
        )
        
        # If organization_name exists and is not empty, it's organizational
        repo_type['is_personal_repo'] = (
            repo_type['organization_name'].isna() | 
            (repo_type['organization_name'] == '') |
            (repo_type['organization_name'].str.strip() == '')
        ).astype(int)
        
        print(f"Using organization data from CSV files")
        print(f"Repositories with organization: {(repo_type['is_personal_repo'] == 0).sum()}")
        print(f"Personal repositories: {repo_type['is_personal_repo'].sum()}")
        
    else:
        # Fallback: Use simple heuristic
        # Extract username from repository name (first part before /)
        repo_type['repo_owner'] = repo_type['repository_name'].str.split('/').str[0]
        
        # If repo owner matches username, likely personal
        # Otherwise, might be organizational (but could also be personal with different name)
        repo_type['is_personal_repo'] = (
            repo_type['repo_owner'].str.lower() == repo_type['username'].str.lower()
        ).astype(int)
        
        repo_type['organization_name'] = None
        repo_type['organization_id'] = None
        
        print(f"Using heuristic classification (repo owner == username)")
        print(f"Repositories classified as personal: {repo_type['is_personal_repo'].sum()}")
        print(f"Repositories classified as potentially organizational: {(repo_type['is_personal_repo'] == 0).sum()}")
        print("Note: This classification is approximate. For better results, use organization_name from enriched data.")
    
    return repo_type[['repository_name', 'username', 'is_personal_repo', 'organization_name']]

def prepare_heterogeneous_did_variables(df, activity_map, repo_type_map):
    """Prepare variables for heterogeneous effects DiD analysis"""
    print("\nPreparing heterogeneous DiD variables...")
    
    # Convert to datetime
    df['event_timestamp'] = pd.to_datetime(df['push_event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date
    
    # Handle missing data
    df['additions'] = pd.to_numeric(df['additions'], errors='coerce')
    df['deletions'] = pd.to_numeric(df['deletions'], errors='coerce')
    df['changes'] = pd.to_numeric(df['changes'], errors='coerce')
    
    # Filter out missing data
    df = df.dropna(subset=['additions', 'deletions', 'changes', 'username', 'country', 'event_timestamp'])
    
    # Aggregate by user and date
    daily_user_stats = df.groupby(['username', 'country', 'date', 'repository_name']).agg({
        'additions': 'sum',
        'deletions': 'sum',
        'changes': 'sum',
        'commit_sha': 'count'
    }).reset_index()
    
    daily_user_stats.rename(columns={'commit_sha': 'commits_count'}, inplace=True)
    
    # Merge activity level
    daily_user_stats = daily_user_stats.merge(
        activity_map[['username', 'country', 'high_activity']],
        on=['username', 'country'],
        how='left'
    )
    
    # Fill missing activity (developers with no pre-ban activity) as low activity
    daily_user_stats['high_activity'] = daily_user_stats['high_activity'].fillna(0).astype(int)
    
    # Merge repository type
    daily_user_stats = daily_user_stats.merge(
        repo_type_map[['repository_name', 'username', 'is_personal_repo']],
        on=['repository_name', 'username'],
        how='left'
    )
    
    # Fill missing repo type as personal (conservative)
    daily_user_stats['is_personal_repo'] = daily_user_stats['is_personal_repo'].fillna(1).astype(int)
    
    # Create treatment group indicator
    daily_user_stats['treatment'] = (daily_user_stats['country'].str.lower() == 'italy').astype(int)
    
    # Create post-treatment indicator
    ban_date = pd.Timestamp('2023-04-01').date()
    daily_user_stats['post_treatment'] = (daily_user_stats['date'] >= ban_date).astype(int)
    
    # Create interaction term
    daily_user_stats['treatment_post'] = daily_user_stats['treatment'] * daily_user_stats['post_treatment']
    
    # Create time-varying effects (days since ban)
    daily_user_stats['date_dt'] = pd.to_datetime(daily_user_stats['date'])
    ban_datetime = pd.Timestamp(ban_date)
    daily_user_stats['days_since_ban'] = (daily_user_stats['date_dt'] - ban_datetime).dt.days
    
    # Create categorical time period variable (mutually exclusive)
    # Before ban: days < 0
    # Immediate: days 0-3
    # Delayed: days 4-7
    # Later: days 8+
    daily_user_stats['time_period'] = 'before'
    daily_user_stats.loc[(daily_user_stats['days_since_ban'] >= 0) & 
                         (daily_user_stats['days_since_ban'] <= 3), 'time_period'] = 'immediate'
    daily_user_stats.loc[(daily_user_stats['days_since_ban'] >= 4) & 
                         (daily_user_stats['days_since_ban'] <= 7), 'time_period'] = 'delayed'
    daily_user_stats.loc[daily_user_stats['days_since_ban'] >= 8, 'time_period'] = 'later'
    
    # Create binary indicators for each time period (for interaction terms)
    daily_user_stats['is_immediate'] = (daily_user_stats['time_period'] == 'immediate').astype(int)
    daily_user_stats['is_delayed'] = (daily_user_stats['time_period'] == 'delayed').astype(int)
    daily_user_stats['is_later'] = (daily_user_stats['time_period'] == 'later').astype(int)
    
    # Create heterogeneous interaction terms
    # Activity level interactions
    daily_user_stats['treatment_post_high_activity'] = (daily_user_stats['treatment_post'] * 
                                                         daily_user_stats['high_activity'])
    daily_user_stats['treatment_post_low_activity'] = (daily_user_stats['treatment_post'] * 
                                                        (1 - daily_user_stats['high_activity']))
    
    # Repository type interactions
    daily_user_stats['treatment_post_personal'] = (daily_user_stats['treatment_post'] * 
                                                      daily_user_stats['is_personal_repo'])
    daily_user_stats['treatment_post_org'] = (daily_user_stats['treatment_post'] * 
                                              (1 - daily_user_stats['is_personal_repo']))
    
    # Time-varying interactions (treatment * time period, only for post-ban periods)
    daily_user_stats['treatment_immediate'] = (daily_user_stats['treatment'] * 
                                              daily_user_stats['is_immediate'])
    daily_user_stats['treatment_delayed'] = (daily_user_stats['treatment'] * 
                                             daily_user_stats['is_delayed'])
    daily_user_stats['treatment_later'] = (daily_user_stats['treatment'] * 
                                           daily_user_stats['is_later'])
    
    # Add day of week controls
    daily_user_stats['day_of_week'] = daily_user_stats['date_dt'].dt.day_name()
    
    # Create log variables
    daily_user_stats['log_additions'] = np.log1p(daily_user_stats['additions'])
    daily_user_stats['log_commits'] = np.log1p(daily_user_stats['commits_count'])
    
    # Convert to standard types
    for col in ['additions', 'deletions', 'changes', 'commits_count']:
        if col in daily_user_stats.columns:
            daily_user_stats[col] = daily_user_stats[col].astype(float)
    
    print(f"Prepared {len(daily_user_stats)} observations")
    
    return daily_user_stats

def heterogeneous_effects_analysis(df):
    """Perform heterogeneous effects DiD analysis"""
    print("\n" + "="*80)
    print("HETEROGENEOUS EFFECTS ANALYSIS")
    print("="*80)
    
    outcomes = {
        'log_additions': 'Log(Lines Added + 1)',
        'log_commits': 'Log(Commits per Day + 1)'
    }
    
    for outcome_var, outcome_name in outcomes.items():
        print(f"\n{'-'*60}")
        print(f"OUTCOME: {outcome_name}")
        print(f"{'-'*60}")
        
        # 1. Activity Level Heterogeneity
        print("\n1. EFFECT BY DEVELOPER ACTIVITY LEVEL:")
        print("-" * 40)
        
        formula = (f"{outcome_var} ~ treatment + post_treatment + treatment_post + "
                  f"treatment_post_high_activity + high_activity + C(day_of_week)")
        
        try:
            model = ols(formula, data=df).fit()
            
            # Get clustered standard errors
            user_groups = df['username'].values
            unique_users = pd.unique(user_groups)
            user_to_idx = {user: idx for idx, user in enumerate(unique_users)}
            group_indices = np.array([user_to_idx[user] for user in user_groups])
            
            try:
                model_clustered = model.get_robustcov_results(cov_type='cluster', groups=group_indices)
            except:
                model_clustered = model.get_robustcov_results(cov_type='HC1')
            
            # Extract coefficients
            params = model.params
            bse = model_clustered.bse
            pvalues = model_clustered.pvalues
            
            # Convert to Series if needed
            if isinstance(params, pd.Series):
                params_series = params
                bse_series = bse if isinstance(bse, pd.Series) else pd.Series(bse, index=params.index)
                pvalues_series = pvalues if isinstance(pvalues, pd.Series) else pd.Series(pvalues, index=params.index)
            else:
                param_names = model.model.exog_names if hasattr(model.model, 'exog_names') else list(range(len(params)))
                params_series = pd.Series(params, index=param_names)
                bse_series = pd.Series(bse, index=param_names) if len(bse) == len(param_names) else pd.Series(bse, index=param_names[:len(bse)])
                pvalues_series = pd.Series(pvalues, index=param_names) if len(pvalues) == len(param_names) else pd.Series(pvalues, index=param_names[:len(pvalues)])
            
            # Base effect (low activity)
            base_effect = params_series.get('treatment_post', 0)
            base_se = bse_series.get('treatment_post', np.nan)
            base_pval = pvalues_series.get('treatment_post', np.nan)
            
            # High activity effect
            high_activity_effect = params_series.get('treatment_post_high_activity', 0)
            high_activity_se = bse_series.get('treatment_post_high_activity', np.nan)
            high_activity_pval = pvalues_series.get('treatment_post_high_activity', np.nan)
            
            # Total effect for high activity
            total_high_effect = base_effect + high_activity_effect
            
            print(f"  Low Activity Developers:")
            print(f"    DiD Effect: {base_effect:.4f} (SE: {base_se:.4f}, p={base_pval:.4f})")
            print(f"  High Activity Developers:")
            print(f"    Additional Effect: {high_activity_effect:.4f} (SE: {high_activity_se:.4f}, p={high_activity_pval:.4f})")
            print(f"    Total Effect: {total_high_effect:.4f}")
            
        except Exception as e:
            print(f"  Error in activity level analysis: {e}")
        
        # 2. Repository Type Heterogeneity
        print("\n2. EFFECT BY REPOSITORY TYPE:")
        print("-" * 40)
        
        formula = (f"{outcome_var} ~ treatment + post_treatment + treatment_post + "
                  f"treatment_post_personal + is_personal_repo + C(day_of_week)")
        
        try:
            model = ols(formula, data=df).fit()
            
            try:
                model_clustered = model.get_robustcov_results(cov_type='cluster', groups=group_indices)
            except:
                model_clustered = model.get_robustcov_results(cov_type='HC1')
            
            params = model.params
            bse = model_clustered.bse
            pvalues = model_clustered.pvalues
            
            # Convert to Series if needed
            if isinstance(params, pd.Series):
                params_series = params
                bse_series = bse if isinstance(bse, pd.Series) else pd.Series(bse, index=params.index)
                pvalues_series = pvalues if isinstance(pvalues, pd.Series) else pd.Series(pvalues, index=params.index)
            else:
                param_names = model.model.exog_names if hasattr(model.model, 'exog_names') else list(range(len(params)))
                params_series = pd.Series(params, index=param_names)
                bse_series = pd.Series(bse, index=param_names) if len(bse) == len(param_names) else pd.Series(bse, index=param_names[:len(bse)])
                pvalues_series = pd.Series(pvalues, index=param_names) if len(pvalues) == len(param_names) else pd.Series(pvalues, index=param_names[:len(pvalues)])
            
            # Personal repos effect
            personal_effect = params_series.get('treatment_post_personal', 0)
            personal_se = bse_series.get('treatment_post_personal', np.nan)
            personal_pval = pvalues_series.get('treatment_post_personal', np.nan)
            
            # Organizational repos effect (base effect)
            org_effect = params_series.get('treatment_post', 0)
            org_se = bse_series.get('treatment_post', np.nan)
            org_pval = pvalues_series.get('treatment_post', np.nan)
            
            # Total effect for personal repos
            total_personal_effect = org_effect + personal_effect
            
            print(f"  Personal Repositories:")
            print(f"    Additional Effect: {personal_effect:.4f} (SE: {personal_se:.4f}, p={personal_pval:.4f})")
            print(f"    Total Effect: {total_personal_effect:.4f}")
            print(f"  Organizational Repositories:")
            print(f"    DiD Effect: {org_effect:.4f} (SE: {org_se:.4f}, p={org_pval:.4f})")
            print("  Note: Repository type classification is approximate. Use organization_name for better results.")
            
        except Exception as e:
            print(f"  Error in repository type analysis: {e}")
        
        # 3. Time-Varying Effects
        print("\n3. TIME-VARYING EFFECTS (IMMEDIATE vs DELAYED):")
        print("-" * 40)
        
        # Use a cleaner specification: treatment + time period main effects + interactions
        # This avoids multicollinearity by using the categorical time_period variable
        formula = (f"{outcome_var} ~ treatment + C(time_period, Treatment(reference='before')) + "
                  f"treatment_immediate + treatment_delayed + treatment_later + C(day_of_week)")
        
        try:
            model = ols(formula, data=df).fit()
            
            try:
                model_clustered = model.get_robustcov_results(cov_type='cluster', groups=group_indices)
            except:
                model_clustered = model.get_robustcov_results(cov_type='HC1')
            
            params = model.params
            bse = model_clustered.bse
            pvalues = model_clustered.pvalues
            
            # Convert to Series if needed
            if isinstance(params, pd.Series):
                params_series = params
                bse_series = bse if isinstance(bse, pd.Series) else pd.Series(bse, index=params.index)
                pvalues_series = pvalues if isinstance(pvalues, pd.Series) else pd.Series(pvalues, index=params.index)
            else:
                param_names = model.model.exog_names if hasattr(model.model, 'exog_names') else list(range(len(params)))
                params_series = pd.Series(params, index=param_names)
                bse_series = pd.Series(bse, index=param_names) if len(bse) == len(param_names) else pd.Series(bse, index=param_names[:len(bse)])
                pvalues_series = pd.Series(pvalues, index=param_names) if len(pvalues) == len(param_names) else pd.Series(pvalues, index=param_names[:len(pvalues)])
            
            immediate_effect = params_series.get('treatment_immediate', 0)
            immediate_se = bse_series.get('treatment_immediate', np.nan)
            immediate_pval = pvalues_series.get('treatment_immediate', np.nan)
            
            delayed_effect = params_series.get('treatment_delayed', 0)
            delayed_se = bse_series.get('treatment_delayed', np.nan)
            delayed_pval = pvalues_series.get('treatment_delayed', np.nan)
            
            later_effect = params_series.get('treatment_later', 0)
            later_se = bse_series.get('treatment_later', np.nan)
            later_pval = pvalues_series.get('treatment_later', np.nan)
            
            print(f"  Immediate Effect (Days 0-3):")
            print(f"    DiD Effect: {immediate_effect:.4f} (SE: {immediate_se:.4f}, p={immediate_pval:.4f})")
            print(f"  Delayed Effect (Days 4-7):")
            print(f"    DiD Effect: {delayed_effect:.4f} (SE: {delayed_se:.4f}, p={delayed_pval:.4f})")
            print(f"  Later Effect (Days 8+):")
            print(f"    DiD Effect: {later_effect:.4f} (SE: {later_se:.4f}, p={later_pval:.4f})")
            
        except Exception as e:
            print(f"  Error in time-varying analysis: {e}")
            import traceback
            traceback.print_exc()

def main():
    print("="*80)
    print("HETEROGENEOUS EFFECTS ANALYSIS")
    print("="*80)
    
    try:
        # Load data
        df = load_and_prepare_data()
        
        # Calculate developer activity levels
        ban_date = pd.Timestamp('2023-04-01').date()
        activity_map = calculate_developer_activity_level(df, ban_date)
        
        # Try to load organization data from CSV files
        org_data = load_organization_data_from_csv()
        
        # Classify repository types (with organization data if available)
        repo_type_map = classify_repository_type(df, org_data=org_data)
        
        # Prepare variables
        df_prepared = prepare_heterogeneous_did_variables(df, activity_map, repo_type_map)
        
        # Perform analysis
        heterogeneous_effects_analysis(df_prepared)
        
        print(f"\n{'='*80}")
        print("ANALYSIS COMPLETE!")
        print(f"{'='*80}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main()

