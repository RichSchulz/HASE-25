#!/usr/bin/env python3
"""
Script to fetch individual commit events from GitHub users using GitHub Archive BigQuery dataset.

This script reads a CSV file containing GitHub users and fetches all individual commit events
that occurred between 60 days before and 60 days after March 27, 2023.

IMPORTANT DATA LIMITATIONS:
- files_added_list, files_removed_list, files_modified_list contain FILE NAMES only, not line counts
- GitHub Archive does not include actual line-by-line diff statistics
- To get line counts, you would need to use the GitHub API or process commit URLs separately
"""

import os
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from dotenv import load_dotenv
import sys

# Load environment variables
load_dotenv(override=True)


def read_users_csv(csv_file_path, sample_size=None):
    """
    Read users from CSV file and optionally sample a specified number of users.
    
    Args:
        csv_file_path (str): Path to the CSV file
        sample_size (int): Number of users to sample (None for all users)
        
    Returns:
        pd.DataFrame: DataFrame containing user data
    """
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
    
    print(f"Reading users from: {csv_file_path}")
    df = pd.read_csv(csv_file_path)
    
    if sample_size is not None:
        # Sample specified number of users randomly
        df = df.sample(n=min(sample_size, len(df)), random_state=42)
        print(f"Sampled {len(df)} users")
    else:
        print(f"Using all {len(df)} users")
    
    return df

def create_bigquery_query(user_table_id, start_date, end_date):
    """
    Create optimized BigQuery query to fetch individual commit events
    from the githubarchive.day dataset.

    This query supports a range of days via _TABLE_SUFFIX filtering
    and joins against a user table for efficiency.

    Args:
        user_table_id (str): Full ID of the BigQuery table holding usernames
                             (e.g., "my_project.my_helpers.user_list_60k")
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        str: BigQuery SQL query
    """

    # Convert YYYY-MM-DD strings to YYYYMMDD for matching _TABLE_SUFFIX
    start_suffix = start_date.replace("-", "")
    end_suffix = end_date.replace("-", "")

    print(f"start_suffix: {start_suffix}, end_suffix: {end_suffix}")

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

def fetch_commit_events(client, query):
    """
    Execute BigQuery query and return results as DataFrame.
    
    Args:
        client: BigQuery client
        query (str): SQL query to execute
        
    Returns:
        pd.DataFrame: Query results
    """
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

def save_results(commit_events_df, output_dir, csv_file_name):
    """
    Save commit events and user subsample to CSV files.
    
    Args:
        commit_events_df (pd.DataFrame): Commit events data
        output_dir (str): Output directory
        csv_file_name (str): Original CSV file name for naming outputs
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filenames
    base_name = os.path.splitext(os.path.basename(csv_file_name))[0]
    commit_events_file = os.path.join(output_dir, f"{base_name}_commit_events.csv")
    
    # Clean commit messages to prevent CSV issues
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


def main():
    # Define date range: 60 days before and after March 27, 2023
    target_date = datetime(2023, 4, 1)
    start_date = target_date - timedelta(days=60)
    end_date = target_date + timedelta(days=60)
    
    print(f"Fetching commit events from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    print(f"Target date: {target_date.strftime('%Y-%m-%d')}")
    print("💡 Using chunked processing to minimize BigQuery costs")
    
    try:
        COUNTRY = "france" # Change as needed
        OUT_DIR = "output"
        CSV_FILE = f"commits_all_{COUNTRY}.csv"

        # IMPORTANT: Define your BigQuery user table ID
        # Replace with your project, dataset, and table name
        USER_TABLE_ID = f"hase-25-project.users.{COUNTRY}" 
        
        print("Initializing BigQuery client...")
        client = bigquery.Client()
        
        # 1. Create the single, efficient query
        query = create_bigquery_query(
            USER_TABLE_ID,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # 2. Run the single query
        # This will scan the data ONCE. Cost will be ~$25-30.
        commit_events_df = fetch_commit_events(client, query) 
        
        # 3. Save results (your existing function is fine)
        commit_events_file = save_results(
            commit_events_df, 
            OUT_DIR, 
            CSV_FILE
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
