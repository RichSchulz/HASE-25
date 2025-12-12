"""
Difference-in-Differences Analysis on LOC Added, Deleted and Total Changes.
Analyzes data 4 weeks prior to the ban and then for both a 2 week period into the ban
and a 4 work-day period into the ban.
"""

import pandas as pd
import numpy as np
import sqlite3
from linearmodels.iv import AbsorbingLS
from typing import cast, Optional
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch


def load_and_prepare_data():
    """
    Filter by date inside SQL to avoid loading massive history.
    """
    print("Loading commit data from SQLite database...")
    
    db_path = f"{Path(__file__).resolve().parent.parent}/large_data/data_commits.sqlite3"
    
    # Calculate safe date buffer for SQL (give extra weeks to be safe)
    # Ban date is 2023-04-01. We need roughly March 1st to April 30th.
    # We filter strictly later in pandas, this is just to save RAM on load.
    sql_start_date = '2023-02-15' 
    sql_end_date = '2023-05-01'

    print(db_path)
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

def print_detailed_results(results, outcome_name, absorb_cols, df_stats):
    """
    Print detailed regression results including all coefficients, intercepts, and group variables
    """
    print(f"\n{'='*80}")
    print(f"DETAILED REGRESSION RESULTS: {outcome_name}")
    print(f"{'='*80}\n")
    
    # Print full summary first
    print("FULL SUMMARY:")
    print(results.summary)
    print("\n")
    
    # Print detailed parameter table
    print("PARAMETER ESTIMATES:")
    print("-" * 80)
    params_df = pd.DataFrame({
        'Coefficient': results.params,
        'Std Error': results.std_errors,
        't-stat': results.tstats,
        'P-value': results.pvalues
    })
    print(params_df.to_string())
    
    # Check which variables were in the original model but are not in results
    print("\nNOTE: In two-way fixed effects models, some variables are automatically dropped:")
    print("  - Intercept (const): Absorbed into user and date fixed effects (not separately identified)")
    print("  - Treatment (Italy): Absorbed into user fixed effects (each user is either Italian or not)")
    print("  - Post-treatment: Absorbed into date fixed effects (each date is either pre or post)")
    print("  - Only the interaction term (treatment_post) and group-specific trends are typically identified")
    print(f"  - Variables actually estimated: {list(results.params.index)}")
    print()
    
    # Print information about absorbed fixed effects
    print("ABSORBED FIXED EFFECTS:")
    print("-" * 80)
    print(f"Fixed effects are absorbed (not explicitly estimated)")
    print(f"Absorbed variables: {', '.join(absorb_cols)}")
    for col in absorb_cols:
        n_unique = df_stats[col].nunique()
        print(f"  - {col}: {n_unique:,} unique values")
    
    # Try to get additional absorbed effects stats if available
    if hasattr(results, 'absorbed_effects_stats'):
        print("\nAdditional absorbed effects statistics:")
        print(results.absorbed_effects_stats)
    print("\n")
    
    # Print model statistics
    print("MODEL STATISTICS:")
    print("-" * 80)
    print(f"Number of observations: {results.nobs:,}")
    print(f"R-squared: {results.rsquared:.6f}")
    if hasattr(results, 'rsquared_within'):
        print(f"R-squared (within): {results.rsquared_within:.6f}")
    print(f"Degrees of freedom: {results.df_resid}")
    print(f"\n{'='*80}\n")

def run_regression(df, outcome_var, outcome_name):
    """
    Run Simple OLS Regression (No FE, No Trend) for verification
    """
    print(f"\nRunning Simple OLS Regression for: {outcome_name}")
    
    # 1. Prepare Data for Simple OLS
    # We need constant, main effects, and interaction
    df = df.copy()
    df['const'] = 1
    
    # Define Regressors (X)
    # Simple DiD: Y = a + b1*Treat + b2*Post + b3*(Treat*Post) + e
    exog_vars = ['const', 'treatment', 'post_treatment', 'treatment_post']
    
    # 3. Define Dependent (Y)
    y = df[outcome_var]
    X = df[exog_vars]
        
    print(f"Running OLS on {len(df)} observations...")
    
    try:
        import statsmodels.api as sm
        
        # Simple OLS
        model = sm.OLS(y, X)
        results = model.fit(cov_type='HC1') # Robust standard errors
        
        # Patch for compatibility with export_results_latex (which expects .std_errors)
        results.std_errors = results.bse
        
        # Print results
        print(results.summary())
        
        return results
        
    except Exception as e:
        print(f"Statsmodels OLS failed: {e}")
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

def create_did_visualization(df, outcome_var, outcome_name, period, treatment_start_date, output_dir, regression_result=None):
    """
    Create a professional Business Style DiD visualization with counterfactual line and effect highlighting.
    """
    print(f"\nCreating DiD visualization for {outcome_name} ({period})...")
    
    # Set professional style
    sns.set_style("whitegrid")
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans', 'Liberation Sans']
    
    # Define pre and post periods
    pre_start = df['date_dt'].min()
    pre_end = treatment_start_date - pd.Timedelta(days=1)
    post_start = treatment_start_date
    post_end = df['date_dt'].max()
    
    # Filter data for pre and post periods
    df_pre = df[df['date_dt'] <= pre_end].copy()
    df_post = df[df['date_dt'] >= post_start].copy()
    
    # Calculate averages for each group and period
    italy_pre = df_pre[df_pre['treatment'] == 1][outcome_var].mean()
    italy_post = df_post[df_post['treatment'] == 1][outcome_var].mean()
    control_pre = df_pre[df_pre['treatment'] == 0][outcome_var].mean()
    control_post = df_post[df_post['treatment'] == 0][outcome_var].mean()
    
    # Calculate counterfactual: What would Italy have been if it followed Control's trend?
    counterfactual_post = italy_pre + (control_post - control_pre)
    
    # Get DiD coefficient and p-value from regression if available
    did_coefficient = None
    did_pvalue = None
    if regression_result is not None and 'treatment_post' in regression_result.params:
        did_coefficient = regression_result.params['treatment_post']
        if 'treatment_post' in regression_result.pvalues:
            did_pvalue = regression_result.pvalues['treatment_post']
    
    # Calculate raw DiD as fallback
    raw_did = (italy_post - italy_pre) - (control_post - control_pre)
    effect_label = did_coefficient if did_coefficient is not None else raw_did
    
    # Calculate treatment post Y-coordinate using counterfactual + coefficient
    # This ensures the visual gap matches the coefficient value
    # This is needed because the regression also controls for differential time trend
    # (treatment_time_trend) and the treament effect is the shift after accounting for that.
    treatment_post_y = counterfactual_post + effect_label
    
    print(f"  Italy (Pre): {italy_pre:.4f}")
    print(f"  Italy (Post - Raw): {italy_post:.4f}")
    print(f"  Italy (Post - Calculated from coefficient): {treatment_post_y:.4f}")
    print(f"  Control (Pre): {control_pre:.4f}")
    print(f"  Control (Post): {control_post:.4f}")
    print(f"  Counterfactual Italy (Post): {counterfactual_post:.4f}")
    print(f"  DiD Effect: {effect_label:.4f}")
    if did_pvalue is not None:
        print(f"  P-value: {did_pvalue:.4f}")
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(11, 7))
    
    # Plot positions
    x_positions = [0, 1]
    periods = ['Pre', 'Post']
    
    # Plot Control line (solid, orange)
    control_values = [control_pre, control_post]
    ax.plot(x_positions, control_values, marker='s', linewidth=2.5, markersize=12, 
            label='Control', color='#FF7F0E', zorder=3, markerfacecolor='white', 
            markeredgewidth=2.5, markeredgecolor='#FF7F0E')
    
    # Plot Counterfactual line (dashed, grey) - from Italy Pre to Counterfactual Post
    counterfactual_values = [italy_pre, counterfactual_post]
    ax.plot(x_positions, counterfactual_values, marker='o', linewidth=2, markersize=10,
            linestyle='--', dashes=(5, 5), label='Counterfactual (Treatment)', 
            color='#808080', alpha=0.7, zorder=2, markerfacecolor='white',
            markeredgewidth=2, markeredgecolor='#808080')
    
    # Plot Actual Italy line (solid, blue) - using calculated treatment_post_y
    italy_values = [italy_pre, italy_post]
    ax.plot(x_positions, italy_values, marker='o', linewidth=2.5, markersize=12,
            label='Treatment (Italy)', color='#1F77B4', zorder=4, markerfacecolor='white',
            markeredgewidth=2.5, markeredgecolor='#1F77B4')
    
    # Add value labels on points
    ax.text(0, italy_pre, f'  {italy_pre:.3f}', va='bottom', ha='left', fontsize=10, fontweight='bold')
    ax.text(1, italy_post, f'  {italy_post:.3f}', va='bottom', ha='left', fontsize=10, fontweight='bold', color='#1F77B4')
    ax.text(0, control_pre, f'  {control_pre:.3f}', va='top', ha='left', fontsize=10, fontweight='bold')
    ax.text(1, control_post, f'  {control_post:.3f}', va='top', ha='left', fontsize=10, fontweight='bold', color='#FF7F0E')
    ax.text(1, counterfactual_post, f'  {counterfactual_post:.3f}', va='top', ha='left', fontsize=9, 
            style='italic', color='#808080', alpha=0.8)
    
    # Draw vertical arrow/bracket showing the treatment effect
    # Position arrow between counterfactual and calculated treatment post
    arrow_y_bottom = min(counterfactual_post, italy_post)
    arrow_y_top = max(counterfactual_post, italy_post)
    arrow_mid = (arrow_y_bottom + arrow_y_top) / 2
    
    # Draw double-headed arrow (grey/black instead of green)
    arrow = FancyArrowPatch(
        (1.15, counterfactual_post), (1.15, italy_post),
        arrowstyle='<->', mutation_scale=20, linewidth=2,
        color='#404040', zorder=5
    )
    ax.add_patch(arrow)
    
    # Add effect label next to arrow with statistical significance note
    effect_sign = '+' if effect_label >= 0 else ''
    label_text = f'{effect_sign}{effect_label:.4f}'
    
    # Add statistical significance note
    if did_pvalue is not None:
        if did_pvalue > 0.1:
            sig_note = 'Diff not statistically\nsignificant (p > 0.1)'
        elif did_pvalue > 0.05:
            sig_note = 'Diff not statistically\nsignificant (p > 0.05)'
        elif did_pvalue > 0.01:
            sig_note = 'Statistically significant\n(p < 0.05)'
        else:
            sig_note = 'Statistically significant\n(p < 0.01)'
    else:
        sig_note = 'Coefficient shown'
    
    # Create label with coefficient and significance note
    full_label = f'{label_text}\n{sig_note}'
    
    ax.text(1.25, arrow_mid, full_label, 
            va='center', ha='left', fontsize=10, fontweight='bold',
            color='#404040', bbox=dict(boxstyle='round,pad=0.4', 
            facecolor='white', edgecolor='#404040', linewidth=1.5))
    
    # Set x-axis labels
    ax.set_xticks(x_positions)
    ax.set_xticklabels(periods, fontsize=12, fontweight='bold')
    
    # Dynamic Y-axis limits - focus tightly on data range
    # Use calculated treatment_post_y instead of raw italy_post
    all_values = [italy_pre, italy_post, control_pre, control_post, counterfactual_post]
    y_min = min(all_values)
    y_max = max(all_values)
    y_range = y_max - y_min
    y_padding = y_range * 0.15  # 15% padding on each side
    ax.set_ylim(y_min - y_padding, y_max + y_padding)
    
    # Add vertical line at treatment start
    ax.axvline(x=0.5, color='#D32F2F', linestyle=':', linewidth=2, alpha=0.6, zorder=1)
    ax.text(0.5, ax.get_ylim()[1] - (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.05, 
            'Treatment\nStarts', ha='center', va='top', fontsize=9, 
            color='#D32F2F', style='italic', bbox=dict(boxstyle='round,pad=0.3',
            facecolor='white', edgecolor='#D32F2F', linewidth=1, alpha=0.8))
    
    # Styling
    ax.set_xlabel('Period', fontsize=13, fontweight='bold', labelpad=10)
    ax.set_ylabel(f'Average {outcome_name}', fontsize=13, fontweight='bold', labelpad=10)
    ax.set_title(f'Difference-in-Differences: {outcome_name}\n({period.replace("_", " ").title()})', 
                 fontsize=15, fontweight='bold', pad=20)
    
    # Professional legend
    ax.legend(loc='upper left', fontsize=11, framealpha=0.95, 
              edgecolor='gray', fancybox=True, shadow=True)
    
    # Grid styling
    ax.grid(True, alpha=0.3, linestyle='--', linewidth=0.8)
    ax.set_axisbelow(True)
    
    # Remove top and right spines for cleaner look
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.5)
    ax.spines['bottom'].set_linewidth(1.5)
    
    plt.tight_layout()
    
    # Save the plot
    period_clean = period.replace('_', '_')
    filename = f"loc_simplified_before_after_{outcome_var}_{period_clean}.png"
    filepath = output_dir / filename
    plt.savefig(filepath, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"  Saved visualization to: {filepath}")
    plt.close()
    
    return {
        'italy_pre': italy_pre,
        'italy_post_raw': italy_post,
        'italy_post_calculated': treatment_post_y,
        'control_pre': control_pre,
        'control_post': control_post,
        'counterfactual_post': counterfactual_post,
        'did_raw': raw_did,
        'did_coefficient': did_coefficient,
        'did_pvalue': did_pvalue
    }

def main():
    # Load Data
    df_raw = load_and_prepare_data()
    
    models_dict = {}
    treatment_periods = ['two_weeks', 'four_weekdays']
    outcomes = {'log_additions': 'Log Lines of Code Added', 'log_deletions': 'Log Lines of Code Deleted'}
    
    # Create output directory for visualizations
    output_dir = Path(__file__).resolve().parent.parent / 'plots'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run Analysis
    for period in treatment_periods:
        print(f"\n{'='*80}")
        print(f"TREATMENT PERIOD: {period.upper().replace('_', ' ')}")
        print(f"{'='*80}")

        # Prepare specific slice (cheap operation now that df_raw is smaller)
        treatment_start = cast(pd.Timestamp, pd.Timestamp('2023-04-01') if period == 'two_weeks' else pd.Timestamp('2023-04-03'))
        
        df = prepare_did_variables(
            df_raw.copy(),
            pre_start=cast(pd.Timestamp, pd.Timestamp('2023-03-04')),
            pre_end=cast(pd.Timestamp, pd.Timestamp('2023-03-31')),
            treatment_start=treatment_start,
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
            
            # Create visualization
            create_did_visualization(
                df, 
                outcome_var, 
                outcome_name, 
                period, 
                treatment_start,
                output_dir,
                regression_result=res
            )
            
    # Export Table
    output_path = f"{Path(__file__).resolve().parent.parent.parent}/final report/parts/loc_simplified_regression_table.tex"
    export_results_latex(models_dict, output_path)

    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE!")
    print(f"{'='*80}")


if __name__ == "__main__":
    main()
