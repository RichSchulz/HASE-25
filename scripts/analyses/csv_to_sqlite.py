"""
Merge data from csv files specified into one sqlite database.
"""

import pandas as pd
import sqlite3
import os
from typing import Any
from pandas.io.sql import SQLiteDatabase, SQLiteTable
from pathlib import Path

def csv_to_df(csv_file: str, extra_columns: dict[str, Any]) -> pd.DataFrame:
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    # pyarrow better handles nullable types
    # default is using numpy which converts int columns to float if it contains null
    # because it converts to NaN which is float
    df = pd.read_csv(csv_file, dtype_backend="pyarrow")

    for col, val in extra_columns.items():
        df[col] = val

    return df


def df_to_sqlite(df: pd.DataFrame, sqlite_db: str, table_name: str):
    conn = sqlite3.connect(sqlite_db)
    # df.to_sql(table_name, conn, if_exists='replace', index=True, index_label='id')

    # Doing what `to_sql`` does internally, but allow setting keys, which is not possible via `to_sql`
    db = SQLiteDatabase(conn)
    table = SQLiteTable(table_name, db, frame=df, if_exists='replace', index=True, index_label='id', keys=['id'])
    table.create()
    table.insert()

    conn.close()


def commits_to_sqlite():
    countries = ['austria', 'france', 'italy']

    merged: pd.DataFrame | None = None
    for country in countries:
        country_df = csv_to_df(
            f"{Path(__file__).resolve().parent.parent}/large_data/commits_all_{country}.csv",
            {
                'country': country,
                'started_at': pd.NaT,
                'completed_at': pd.NaT,
            }
        )

        if merged is None:
            merged = country_df
        else:
            merged = pd.concat([merged, country_df], ignore_index=True)

    if merged is not None:
        # df_to_sqlite(merged.head(100), "large_data/commits_all_sample.sqlite3", 'commits')
        df_to_sqlite(merged, f"{Path(__file__).resolve().parent.parent}/large_data/commits_all.sqlite3", 'commits')


if __name__ == "__main__":
    commits_to_sqlite()
