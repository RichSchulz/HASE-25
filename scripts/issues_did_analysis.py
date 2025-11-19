import pandas as pd
import os
from typing import Any, cast
import matplotlib.pyplot as plt
import seaborn as sns
import datetime
import numpy as np
import statsmodels.formula.api as smf
from typing import Optional


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
    # countries = ["austria", "italy"]

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


def prepare_issue_events(df: pd.DataFrame):
    df = cast(pd.DataFrame, df[df["event_type"] == "IssuesEvent"])

    df['issue_body'] = df['issue_body'].fillna("")
    df['issue_length'] = df['issue_body'].apply(len)

    df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], errors='coerce')

    # Extract date (no time component)
    df['event_date'] = df['event_timestamp'].dt.date

    return df


def issue_length_did_analysis(
    df: pd.DataFrame,
    policy_date: datetime.date = datetime.date(2023, 4, 1),
    pre_days: int = 14,
    post_days: int = 7,
    save_paths: Optional[dict[str, str]] = None,
):
    """Run a simple DiD on issue body length per user per day.

    - Uses `username` as the unit, aggregates total issue length per user-day (sum).
    - Treatment group: `country == 'italy'`.
    - Control group: `country` in `['france','austria']`.
    - Filters to `IssuesEvent` with `action == 'opened'`.
    - Removes outliers in `issue_length` using the IQR rule.
    - Fits OLS: outcome ~ treatment * post + date FE + user FE, clustered SE by user.
    """
    # Keep only opened issues
    df = cast(pd.DataFrame, df[df.get('action') == 'opened'])

    # Restrict countries
    df = cast(pd.DataFrame, df[df['country'].isin(['italy', 'france', 'austria'])])

    # Window around policy date
    start_date = policy_date - datetime.timedelta(days=pre_days)
    end_date = policy_date + datetime.timedelta(days=post_days)
    df = cast(pd.DataFrame, df[(df['event_date'] >= start_date) & (df['event_date'] <= end_date)])

    # Remove extreme outliers by IQR on issue_length
    lengths = df['issue_length'].dropna()
    q1 = lengths.quantile(0.25)
    q3 = lengths.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    df = cast(pd.DataFrame, df[(df['issue_length'] >= lower) & (df['issue_length'] <= upper)])

    # Aggregate per user-day
    agg = (
        df.groupby(['username', 'event_date', 'country'])
        .agg(
            user_day_total_length=('issue_length', 'sum'),
            user_day_count=('issue_length', 'size'),
        )
        .reset_index()
    )

    # DiD indicators
    agg['treatment'] = (agg['country'] == 'italy').astype(int)
    agg['post'] = (agg['event_date'] >= policy_date).astype(int)

    # Fit DiD with user and date fixed effects
    # Note: `C(username)` and `C(event_date)` add user and date fixed effects
    formula = 'user_day_total_length ~ treatment * post + C(event_date) + C(username)'
    model = smf.ols(formula=formula, data=agg)
    try:
        res = model.fit(cov_type='cluster', cov_kwds={'groups': agg['username']})
    except Exception:
        # Fallback to plain OLS if clustering fails
        res = model.fit()

    # Save outputs if requested
    if save_paths is None:
        save_paths = {}
    agg_path = save_paths.get('agg', 'data/issues_did_agg.csv')
    model_path = save_paths.get('model', 'data/issues_did_model_summary.txt')

    os.makedirs(os.path.dirname(agg_path), exist_ok=True)
    agg.to_csv(agg_path, index=False)

    with open(model_path, 'w') as fh:
        fh.write(res.summary().as_text())

    print(res.summary())
    return agg, res


def main():
    df = read_and_merge_dfs()
    df = prepare_issue_events(df)
    # Run DiD analysis (default: 7 days before and after 2023-04-01)
    agg, res = issue_length_did_analysis(df)

if __name__ == "__main__":
    main()
