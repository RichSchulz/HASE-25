import pandas as pd
import os
from typing import Any, cast
import matplotlib.pyplot as plt
import seaborn as sns
import re


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


def prepare_bug_events(df: pd.DataFrame):
    # customize keywords as needed
    keywords = ["bug", "fix", "error", "crash"]
    pattern = r"\b(" + "|".join(map(re.escape, keywords)) + r")\b"

    # df['is_bug'] = df['issue_body'].str.contains(pattern, case=False, na=False, regex=True)
    df['is_bug'] = df['issue_body'].str.contains(pattern, case=False, na=False, regex=True) | df['issue_labels'].str.contains(pattern, case=False, na=False, regex=True)

    return df


def plot_bug_issues(df: pd.DataFrame, window: int = 30, resample_freq: str = "W"):
    df = df.copy()
    df['event_date'] = pd.to_datetime(df['event_date'])
    parts = []

    for country, sub in df.groupby('country'):
        sub = sub.set_index('event_date').sort_index()
        # sum = number of bug issues in the period; count = total issues in the period
        agg = cast(pd.DataFrame, sub['is_bug'].resample(resample_freq).agg(['sum', 'count']))
        agg = agg.rename(columns={'sum': 'bug_count', 'count': 'total_count'})
        # avoid division by zero periods
        agg = agg[agg['total_count'] > 0].copy()
        agg['bug_ratio'] = agg['bug_count'] / agg['total_count']
        agg = agg.reset_index()
        agg['country'] = country
        parts.append(agg)

    if not parts:
        raise ValueError("No data to plot after resampling")

    plot_df = pd.concat(parts, ignore_index=True)

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=plot_df,
        x='event_date',
        y='bug_ratio',
        hue='country',
        linewidth=2
    )

    plt.title(f"Ratio of Bug-related Issues Over Time by Country (resample={resample_freq})")
    plt.xlabel("Date")
    plt.ylabel("Fraction of Bug-related Issues")
    plt.ylim(0, 1)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("data/bug_issues_ratio.png")


def main():
    df = read_and_merge_dfs()
    df = prepare_issue_events(df)
    df = prepare_bug_events(df)
    plot_bug_issues(df)


if __name__ == "__main__":
    main()
