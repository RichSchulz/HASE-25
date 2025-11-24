import pandas as pd
import numpy as np
import os
from linearmodels.iv import AbsorbingLS
from typing import Any, cast


def csv_to_df(csv_file: str, extra_columns: dict[str, Any]) -> pd.DataFrame:
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # pyarrow better handles nullable types
    # default is using numpy which converts int columns to float if it contains null
    # because it converts to NaN which is float
    df = pd.read_csv(csv_file, dtype_backend="pyarrow")

    # we need to convert back to numpy so smf can handle it
    df = df.convert_dtypes(dtype_backend="numpy_nullable")

    for col, val in extra_columns.items():
        df[col] = val

    return df


def read_and_merge_dfs() -> pd.DataFrame:
    countries = ["austria", "france", "italy"]

    merged: pd.DataFrame | None = None
    for country in countries:
        country_df = csv_to_df(
            f"large_data/events_all_{country}.csv",
            {
                "country": country,
            }
        )

        if merged is None:
            merged = country_df
        else:
            merged = pd.concat([merged, country_df], ignore_index=True)

    if merged is None:
        raise ValueError("DataFrame could not be read")

    return merged


def prepare_did_variables(
        df: pd.DataFrame,
        pre_start: pd.Timestamp,
        pre_end: pd.Timestamp,
        treatment_start: pd.Timestamp,
        treatment_end: pd.Timestamp,
        check_weekday: bool
):
    pre_start_date = pre_start.date()
    pre_end_date = pre_end.date()
    treatment_start_date = treatment_start.date()
    treatment_end_date = treatment_end.date()

    df = cast(pd.DataFrame, df[df["event_type"] == "PullRequestEvent"])
    df = cast(pd.DataFrame, df[df["action"] == "opened"])

    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'])
    df['date'] = df['event_timestamp'].dt.date

    df = cast(pd.DataFrame, df[(df['date'] >= pre_start_date) & (df['date'] < treatment_end_date)])
    
    # Aggregate
    print("Aggregating pull requests by user and date...")
    daily_user_stats = df.groupby(['username', 'country', 'date']).agg({
        'pr_number': 'count'
    }).reset_index()
    daily_user_stats.rename(columns={'pr_number': 'prs_opened'}, inplace=True)

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

    daily_user_stats['log_prs_opened'] = np.log1p(daily_user_stats['prs_opened'])

    # Categoricals for efficiency
    daily_user_stats['username'] = daily_user_stats['username'].astype('category')
    daily_user_stats['date'] = daily_user_stats['date'].astype('category')
    
    return daily_user_stats


def run_fast_regression(df, outcome_var, outcome_name):
    """
    OPTIMIZATION: Uses linearmodels AbsorbingLS for speed.
    Absorbs Fixed Effects instead of creating dummy columns.
    """
    print(f"\nRunning Fast Regression for: {outcome_name}")

    # Prepare data for Linearmodels
    # We need to separate the "absorbed" effects (FE) from the regressors
    
    # 1. Define Fixed Effects (Absorb)
    # Note: 'date' absorbs 'day_of_week', so we don't need both strictly, 
    # but we can include day_of_week if needed. 'date' + 'username' is standard TWFE.
    absorb_cols = ['username', 'date'] 
    
    # 2. Define Regressors (X)
    # We need the interaction (DiD estimator) and the group-specific trend
    exog_vars = ['treatment_post', 'treatment_time_trend']
    
    # 3. Define Dependent (Y)
    y = df[outcome_var]
    X = df[exog_vars]
    
    # Ensure constant is handled (AbsorbingLS usually centers data, but we can add constant if needed)
    # For pure DiD with FE, we care about the slope of interaction.
    
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
        f.write(r"\begin{table}[htbp]" + "\n")
        f.write(r"\centering" + "\n")
        f.write(r"\caption{Difference-in-Differences Results (Absorbed Fixed Effects)}" + "\n")
        f.write(r"\begin{tabular}{l|cc}" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r" & \multicolumn{2}{c}{\textbf{Log Pull Requests Opened}} \\" + "\n")
        f.write(r" & 2 Weeks & 4 Weekdays \\" + "\n")
        f.write(r"\hline" + "\n")
        
        # Row: Treatment x Post
        row_coef = "Italy $\\times$ Post"
        row_se = ""
        
        # Iterate through the 4 configurations
        configs = [
            ('log_prs_opened', 'two_weeks'),
            ('log_prs_opened', 'four_weekdays'),
        ]
        
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
        f.write(r"User FE & Yes & Yes \\" + "\n")
        f.write(r"Date FE & Yes & Yes \\" + "\n")
        f.write(r"\hline \hline" + "\n")
        f.write(r"\end{tabular}" + "\n")
        f.write(r"\end{table}" + "\n")


# Re-use the plotting functions from original script (omitted here for brevity, they were fine)
def plot_lines_added_before_after(df):
    # (Keep your original plotting code here, it performs fine on aggregated data)
    pass

def main():
    df_raw = read_and_merge_dfs()
    
    models_dict = {}
    treatment_periods = ['two_weeks', 'four_weekdays']
    outcomes = {
        'log_prs_opened': 'Log Pull Requests Opened',
    }
    
    # 2. Run Analysis
    for period in treatment_periods:
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
            # Run Optimized Regression
            res = run_fast_regression(df, outcome_var, outcome_name)
            models_dict[(outcome_var, period)] = res
            
            # Print Quick Summary
            print(res.summary)
            
    # 3. Export Table
    output_path = "../final report/parts/prs_opened_did_table.tex"
    export_results_latex(models_dict, output_path)


if __name__ == "__main__":
    main()
