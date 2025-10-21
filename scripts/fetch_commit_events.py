import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
import sys


def create_bigquery_query(user_table_id: str, start_date: str, end_date: str) -> str:
    # Convert YYYY-MM-DD strings to YYYYMMDD for matching _TABLE_SUFFIX
    start_suffix = start_date.replace("-", "")
    end_suffix = end_date.replace("-", "")

    query = f"""
    WITH push_events AS (
        SELECT
            actor.login AS username,
            repo.name AS repository_name,
            repo.id AS repository_id,
            created_at AS event_timestamp,
            JSON_EXTRACT_ARRAY(payload, "$.commits") AS commits,
            JSON_VALUE(payload, "$.ref") AS branch_name,
            JSON_VALUE(payload, "$.size") AS push_size,
            JSON_VALUE(payload, "$.distinct_size") AS distinct_commits,
            JSON_VALUE(payload, "$.head") AS head_commit_sha,
            JSON_VALUE(payload, "$.before") AS before_commit_sha,
            org.login AS organization_name,
            org.id AS organization_id
        FROM
            `githubarchive.day.20*`
        INNER JOIN
            `{user_table_id}` AS users
        ON actor.login = users.login
        WHERE
            type = 'PushEvent'
            AND _TABLE_SUFFIX BETWEEN '{start_suffix[2:]}' AND '{end_suffix[2:]}'
    ),

    commit_events AS (
        SELECT
            username,
            repository_name,
            repository_id,
            organization_name,
            organization_id,
            event_timestamp,
            branch_name,
            push_size,
            distinct_commits,
            head_commit_sha,
            before_commit_sha,
            JSON_VALUE(commit, "$.sha") AS commit_sha,
            JSON_VALUE(commit, "$.message") AS commit_message,
            JSON_VALUE(commit, "$.author.name") AS commit_author_name,
            JSON_VALUE(commit, "$.author.email") AS commit_author_email,
        FROM
            push_events,
            UNNEST(commits) AS commit
    )

    SELECT
        username,
        repository_name,
        repository_id,
        organization_name,
        organization_id,
        event_timestamp,
        branch_name,
        commit_sha,
        commit_message,
        commit_author_name,
        commit_author_email,
        push_size,
        distinct_commits,
        head_commit_sha,
        before_commit_sha
    FROM
        commit_events
    ORDER BY
        event_timestamp DESC
    """

    return query


def fetch_commit_events(client: bigquery.Client, query: str) -> pd.DataFrame:
    print("Executing BigQuery query...")
    print("This may take several minutes depending on the data size...")
    
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        print(f"✅ Retrieved {len(df)} commit events")
        return df
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        raise


def save_results(commit_events_df: pd.DataFrame, output_dir: str, csv_file_name: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    commit_events_file = os.path.join(output_dir, csv_file_name)
    
    # Clean commit messages to prevent CSV issues
    # TODO: Should we really do this? I think it would not be necessary
    if 'commit_message' in commit_events_df.columns:
        commit_events_df = commit_events_df.copy()
        commit_events_df['commit_message'] = commit_events_df['commit_message'].fillna('').astype(str)
        # Replace newlines and carriage returns with spaces
        commit_events_df['commit_message'] = commit_events_df['commit_message'].str.replace('\n', ' ', regex=False)
        commit_events_df['commit_message'] = commit_events_df['commit_message'].str.replace('\r', ' ', regex=False)
        # Replace multiple spaces with single space
        commit_events_df['commit_message'] = commit_events_df['commit_message'].str.replace(r'\s+', ' ', regex=True)
        # Strip leading/trailing whitespace
        commit_events_df['commit_message'] = commit_events_df['commit_message'].str.strip()
    
    # Save commit events with proper CSV escaping
    commit_events_df.to_csv(commit_events_file, index=False, encoding="utf-8", quoting=1, escapechar='\\')
    print(f"✅ Saved {len(commit_events_df)} commit events to: {commit_events_file}")
    
    return commit_events_file


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

    # Define date range: 60 days before and after March 27, 2023
    target_date = datetime(2023, 4, 1)
    start_date = target_date - timedelta(days=60)
    end_date = target_date + timedelta(days=60)
    
    print(f"Fetching commit events from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Target date: {target_date.strftime('%Y-%m-%d')}")
    print("💡 Using chunked processing to minimize BigQuery costs")
    
    try:
        country = "france" # Change as needed
        out_dir = "large_data"
        csv_file = f"new__commits_all_{country}.csv"

        user_table_id = f"hase-25-project.users.{country}" 
        
        print("Initializing BigQuery client...")
        client = bigquery.Client()
        
        query = create_bigquery_query(
            user_table_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        commit_events_df = fetch_commit_events(client, query) 
        
        commit_events_file = save_results(
            commit_events_df, 
            out_dir, 
            csv_file
        )
        
        print("\n📊 Summary:")
        print(f"  - Commit events found: {len(commit_events_df)}")
        print(f"  - Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"  - Output files:")
        print(f"    - Commit events: {commit_events_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
