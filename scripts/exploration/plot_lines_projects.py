"""
Plot LOC added and deleted, as well as commit volume, of some manually selected Italian projects.
"""

import pandas as pd
import os
from typing import cast
import matplotlib.pyplot as plt
import sqlite3
from pathlib import Path


def sqlite_to_df(sqlite_file: str, country: str) -> pd.DataFrame:
    if not os.path.exists(sqlite_file):
        raise FileNotFoundError(f"sqlite file not found: {sqlite_file}")
    
    conn = sqlite3.connect(sqlite_file)

    # pyarrow better handles nullable types
    # default is using numpy which converts int columns to float if it contains null
    # because it converts to NaN which is float
    df = pd.read_sql_query("SELECT * FROM commit_data WHERE country = :country", params={"country": country}, con=conn, dtype_backend="pyarrow")

    # we need to convert back to numpy so smf can handle it
    df = df.convert_dtypes(dtype_backend="numpy_nullable")

    return df


def aggregate_commits_per_day(df: pd.DataFrame) -> pd.DataFrame:
    df['date'] = pd.to_datetime(df['push_event_timestamp'], errors='coerce').dt.normalize()

    # Coerce to numeric and fill NaN with 0
    df['additions'] = cast(pd.Series, pd.to_numeric(df['additions'], errors='coerce')).fillna(0).astype('int64')
    df['deletions'] = cast(pd.Series, pd.to_numeric(df['deletions'], errors='coerce')).fillna(0).astype('int64')

    # Sum additions and deletions per day
    daily_sum = cast(pd.Series, df.groupby('date').agg(
        additions=('additions', 'sum'),
        deletions=('deletions', 'sum')
    ))

    # Count commits (rows) per day
    daily_count = cast(pd.Series, df.groupby('date').size()).rename('commits')

    # Combine into single dataframe
    daily_total = pd.concat([daily_sum, daily_count], axis=1).reset_index().fillna(0)

    # Ensure integer types for summed columns
    daily_total['additions'] = daily_total['additions'].astype('int64')
    daily_total['deletions'] = daily_total['deletions'].astype('int64')
    daily_total['commits'] = daily_total['commits'].astype('int64')

    daily_total = daily_total.sort_values('date')

    return daily_total


def plot_commits_per_day(daily_total: pd.DataFrame, country: str, output_file: str) -> None:
        # Resample to weekly and smooth so curves are clear in the plot.
        plt.figure(figsize=(12, 6))

        df = daily_total.copy()
        # Ensure date column is datetime and set as index
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.set_index('date').sort_index()

        # Resample to weekly bins (week starting Monday) and sum values in each week
        weekly = df.resample('W-MON').sum()

        # Interpolate small gaps to avoid choppy smoothing
        weekly = weekly.interpolate(method='time', limit_direction='both')

        # Apply centered rolling mean (3-week window default) for smoothing
        smooth_w = 3
        weekly['additions_smoothed'] = weekly['additions'].rolling(window=smooth_w, center=True, min_periods=1).mean()
        weekly['deletions_smoothed'] = weekly['deletions'].rolling(window=smooth_w, center=True, min_periods=1).mean()
        weekly['commits_smoothed'] = weekly['commits'].rolling(window=smooth_w, center=True, min_periods=1).mean()

        # Plot raw weekly totals (light) and smoothed curves (emphasized)
        dates = weekly.index

        plt.plot(dates, weekly['additions'], marker='o', linestyle='-', alpha=0.25, color='tab:green', label='Additions (weekly)')
        plt.plot(dates, weekly['additions_smoothed'], color='tab:green', linewidth=2.2, label=f'Additions (smoothed {smooth_w}w)')

        plt.plot(dates, weekly['deletions'], marker='o', linestyle='-', alpha=0.25, color='tab:red', label='Deletions (weekly)')
        plt.plot(dates, weekly['deletions_smoothed'], color='tab:red', linewidth=2.2, label=f'Deletions (smoothed {smooth_w}w)')

        plt.plot(dates, weekly['commits'], marker='o', linestyle='-', alpha=0.25, color='tab:blue', label='Commits (weekly)')
        plt.plot(dates, weekly['commits_smoothed'], color='tab:blue', linewidth=2.2, label=f'Commits (smoothed {smooth_w}w)')

        plt.xlabel('Date')
        plt.ylabel('Count / Lines')
        plt.title(f'Weekly additions, deletions and commits for filtered projects ({country})')
        plt.legend(ncol=2)
        plt.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(output_file)


def main():
    country = "italy"

    df_commits = sqlite_to_df(
        sqlite_file=f"{Path(__file__).resolve().parent}/large_data/data_commits_projects.sqlite3",
        country=country
    )
    
    df_daily = aggregate_commits_per_day(df=df_commits)
    plot_commits_per_day(
        daily_total=df_daily,
        country=country,
        output_file=f"{Path(__file__).resolve().parent}/plots/lines_projects_{country}.png"
    )


if __name__ == "__main__":
    main()
