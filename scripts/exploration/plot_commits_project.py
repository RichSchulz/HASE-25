"""
Plot number of commits of some manually selected, representative Italian projects.
"""

import pandas as pd
import os
from typing import cast, Any
import matplotlib.pyplot as plt
from pathlib import Path


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


def filter_events(df_events: pd.DataFrame) -> pd.DataFrame:
    df = cast(pd.DataFrame, df_events[
        (df_events['event_type'] == 'PushEvent')
    ]).copy()

    return df


def merge_ratios(
        df_events: pd.DataFrame,
        df_ratios: pd.DataFrame
) -> pd.DataFrame:
    df_merged = pd.merge(
        left=df_ratios,
        right=df_events,
        how='left',
        left_on='repo',
        right_on='repository_name'
    )

    return df_merged


def filter_ratios(df: pd.DataFrame, min_commits: int, min_ratio: float) -> pd.DataFrame:
    df_filtered = cast(pd.DataFrame, df[
        (df['total_commits'] >= min_commits) &
        (df['country_ratio'] >= min_ratio)
    ]).copy()

    df_filtered = df_filtered.sort_values(by='total_commits', ascending=False)

    return df_filtered


def filter_projects(df: pd.DataFrame) -> pd.DataFrame:
    df_filtered = cast(pd.DataFrame, df[
        (df['is_code'] == 1)
    ]).copy()
    return df_filtered


def aggregate_commits_per_project(df: pd.DataFrame) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df['event_timestamp'], errors='coerce').dt.normalize()
        
    # commits per project per day
    per_proj_day = df.groupby(['repository_name', 'date']).size().reset_index().rename(columns={0: 'commits'})

    # total commits per day across the filtered projects
    daily_total = cast(pd.DataFrame, per_proj_day.groupby('date')['commits'].sum().reset_index())

    # Ensure sorted by date
    daily_total = daily_total.sort_values('date')

    # Resample to 1 week bins (week starting Monday). This sums daily commits into weekly totals.
    weekly_total = (
        daily_total
        .set_index('date')
        .resample('W-MON')['commits']
        .sum()
        .reset_index()
    )

    # Add a smoothed series: centered rolling mean across 3 weeks by default.
    # You can change the window in plot_commits_per_day if you want a different smoothing.
    weekly_total['smoothed_commits'] = (
        weekly_total['commits']
        .rolling(window=3, center=True, min_periods=1)
        .mean()
    )

    return weekly_total


def plot_commits_per_day(daily_total: pd.DataFrame, country: str, output_file: str) -> None:
    plt.figure(figsize=(12, 5))

    # The function now expects a weekly-resampled dataframe with 'commits' and 'smoothed_commits'
    if 'smoothed_commits' in daily_total.columns:
        # Plot raw weekly totals lightly
        plt.plot(daily_total['date'], daily_total['commits'], marker='o', linestyle='-', alpha=0.4, label='Weekly total')
        # Plot smoothed line emphasized
        plt.plot(daily_total['date'], daily_total['smoothed_commits'], color='tab:blue', linewidth=2.5, label='Smoothed (3-week rolling)')
    else:
        plt.plot(daily_total['date'], daily_total['commits'], marker='o', linestyle='-')

    plt.xlabel('Date')
    plt.ylabel('Commits')
    plt.title(f'Total commits per week for filtered projects ({country})')
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_file)


def main():
    country = "italy"

    df_events = csv_to_df(
        csv_file=f"{Path(__file__).resolve().parent}/large_data/events_projects_{country}.csv",
        extra_columns={
            "country": country,
        }
    )
    df_events = filter_events(df_events=df_events)

    # Use this to first filter projects cased on min_commits and min_ratio

    # df_ratios = csv_to_df(
    #     csv_file=f"{Path(__file__).resolve().parent}/large_data/projects_ratio_{country}.csv",
    #     extra_columns={}
    # )
    # df_ratios = filter_ratios(
    #     df=df_ratios,
    #     min_commits=50,
    #     min_ratio=0.75
    # )
    # df_ratios["repo_url"] = "https://github.com/" + df_ratios["repo"]
    # df_ratios.to_csv(f"{Path(__file__).resolve().parent}/data/filtered_projects_{country}.csv", index=False)

    # Then, projects have been manually selected in filtered_manually_projects_italy.csv

    df_ratios_manually_filtered = csv_to_df(
        csv_file=f"{Path(__file__).resolve().parent}/data/filtered_manually_projects_{country}.csv",
        extra_columns={}
    )
    df_ratios_manually_filtered = filter_projects(df=df_ratios_manually_filtered)

    print(f"Count of manually filtered projects: {len(df_ratios_manually_filtered)}")

    df = merge_ratios(
        df_events=df_events,
        df_ratios=df_ratios_manually_filtered
    )
    df.to_csv(f"{Path(__file__).resolve().parent}/data/only_filtered_manually_projects_{country}.csv", index=False)
    
    df_daily = aggregate_commits_per_project(df=df)
    plot_commits_per_day(
        daily_total=df_daily,
        country=country,
        output_file=f"{Path(__file__).resolve().parent}/plots/commits_per_day_{country}.png"
    )


if __name__ == "__main__":
    main()
