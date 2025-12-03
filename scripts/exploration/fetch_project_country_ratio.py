"""
Fetch PushEvents from GH Archive, from repositories where at least 25% of the commits
during a given timeframe have been made by contributors based in Italy, and save them
to a csv file.
"""

import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv
import sys
from pathlib import Path


def create_bigquery_query(user_table_id: str, start_date: str, end_date: str) -> str:
    # Convert YYYY-MM-DD strings to YYYYMMDD for matching _TABLE_SUFFIX
    start_suffix = start_date.replace("-", "")
    end_suffix = end_date.replace("-", "")

    query = f"""
    WITH commits AS (
    SELECT
        repo.name AS repo,
        actor.login AS user_login,
        COUNT(*) AS commit_count
    FROM
        `githubarchive.day.20*`
    WHERE
        _TABLE_SUFFIX BETWEEN '{start_suffix[2:]}' AND '{end_suffix[2:]}'
        AND type = 'PushEvent'
    GROUP BY repo, user_login
    ),
    repo_totals AS (
    SELECT repo, SUM(commit_count) AS total_commits
    FROM commits GROUP BY repo
    ),
    country_commits AS (
    SELECT c.repo, SUM(c.commit_count) AS country_commits
    FROM commits c
    JOIN `{user_table_id}` u
        ON LOWER(c.user_login) = LOWER(u.login)
    GROUP BY c.repo
    )
    SELECT
    i.repo,
    i.country_commits,
    r.total_commits,
    SAFE_DIVIDE(i.country_commits, r.total_commits) AS country_ratio
    FROM country_commits i
    JOIN repo_totals r ON i.repo = r.repo
    WHERE SAFE_DIVIDE(i.country_commits, r.total_commits) >= 0.25
    ORDER BY country_ratio DESC
    """

    return query


def fetch_results(client: bigquery.Client, query: str) -> pd.DataFrame:
    print("Executing BigQuery query...")
    print("This may take several minutes depending on the data size...")
    
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        print(f"✅ Retrieved {len(df)} entries")
        return df
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        raise


def save_results(results_df: pd.DataFrame, output_dir: str, csv_file_name: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_csv_file = os.path.join(output_dir, csv_file_name)
    
    results_df.to_csv(output_csv_file, index=False, encoding="utf-8", quoting=1, escapechar='\\')
    print(f"✅ Saved {len(results_df)} results to: {output_csv_file}")
    
    return output_csv_file


def confirm_action(prompt: str):
    while True:
        answer = input(f"{prompt} (y/N)").strip().lower()
        if answer in ("n", ""):
            return False
        elif answer == "y":
            return True
        else:
            print("Please enter 'y' or 'n' (default is 'N').")

def main():
    load_dotenv(override=True)

    if not confirm_action("💰💰💰 This query is expensive and might override existing data. Do you want to continue?"):
        print("👋 bye")
        return

    start_date = datetime(2022, 10, 1)
    end_date = datetime(2023, 9, 30)

    print(f"Fetching results from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        country = "italy" # Change as needed
        out_dir = f"{Path(__file__).resolve().parent}/large_data"
        csv_file = f"projects_ratio_{country}.csv"

        user_table_id = f"hase-25-project.users.{country}" 

        print("Initializing BigQuery client...")
        client = bigquery.Client()
        
        query = create_bigquery_query(
            user_table_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        results_df = fetch_results(client, query) 
        
        results_file = save_results(
            results_df, 
            out_dir, 
            csv_file
        )
        
        print("\n📊 Summary:")
        print(f"  - Entries found: {len(results_df)}")
        print(f"  - Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"  - Output files:")
        print(f"  - {results_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
