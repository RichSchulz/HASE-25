import pandas as pd
import os
from typing import Any, cast
import matplotlib.pyplot as plt
import seaborn as sns


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


def plot_release_events(df: pd.DataFrame, resample_freq: str = "W"):
    df = df.copy()
    df['event_date'] = pd.to_datetime(df['event_date'])

    # Keep only top N countries by number of issues
    countries = df['country'].value_counts().index
    parts = []

    for country, sub in df[df['country'].isin(countries)].groupby('country'):
        sub = sub.set_index('event_date').sort_index()
        # resample to period and compute mean issue length
        resampled = sub['issue_length'].resample(resample_freq).mean().rename('issue_length').to_frame()
        resampled = resampled.reset_index()
        resampled['country'] = country
        parts.append(resampled)

    if not parts:
        raise ValueError("No data to plot after resampling")

    plot_df = pd.concat(parts, ignore_index=True)

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=plot_df,
        x='event_date',
        y='issue_length',
        hue='country',
        linewidth=2
    )

    plt.title(f"Mean Issue Body Length per {resample_freq} by Country")
    plt.xlabel("Date")
    plt.ylabel("Mean Issue Body Length (characters)")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("data/mean_issue_length.png")


def main():
    df = read_and_merge_dfs()
    df = prepare_issue_events(df)
    plot_release_events(df)


if __name__ == "__main__":
    main()
