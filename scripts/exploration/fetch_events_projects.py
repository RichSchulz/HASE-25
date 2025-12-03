"""
Fetches events from GH Archive for some manually selected projects in Italy, which show a high
percentage of Italian developers and high coding activity.
"""

import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv
import sys
from pathlib import Path


def create_bigquery_query(project_table_id: str, start_date: str, end_date: str) -> str:
    # Convert YYYY-MM-DD strings to YYYYMMDD for matching _TABLE_SUFFIX
    start_suffix = start_date.replace("-", "")
    end_suffix = end_date.replace("-", "")

    query = f"""
    WITH events AS (
        SELECT
            gh.id AS event_id,
            gh.actor.login AS username,
            gh.repo.name AS repository_name,
            gh.repo.id AS repository_id,
            gh.type AS event_type,
            gh.created_at AS event_timestamp,
            gh.org.login AS organization_name,
            gh.org.id AS organization_id,
            JSON_VALUE(gh.payload, "$.release.id") AS release_id,
            JSON_EXTRACT_ARRAY(gh.payload, "$.commits") AS commits,
            JSON_VALUE(gh.payload, "$.ref") AS branch_name,
            JSON_VALUE(gh.payload, "$.size") AS push_size,
            JSON_VALUE(gh.payload, "$.distinct_size") AS distinct_commits,
            JSON_VALUE(gh.payload, "$.head") AS head_commit_sha,
            JSON_VALUE(gh.payload, "$.before") AS before_commit_sha,
            JSON_VALUE(gh.payload, "$.action") AS action,
            JSON_VALUE(gh.payload, "$.issue.number") AS issue_number,
            JSON_VALUE(gh.payload, "$.issue.title") AS issue_title,
            JSON_VALUE(gh.payload, "$.issue.body") AS issue_body,
            ARRAY(
                SELECT JSON_VALUE(label, "$.name")
                FROM UNNEST(JSON_EXTRACT_ARRAY(gh.payload, "$.issue.labels")) AS label
            ) AS issue_label,
            JSON_VALUE(gh.payload, "$.pull_request.number") AS pr_number,
            JSON_VALUE(gh.payload, "$.pull_request.title") AS pr_title,
            JSON_VALUE(gh.payload, "$.pull_request.body") AS pr_body,
            ARRAY(
                SELECT JSON_VALUE(label, "$.name")
                FROM UNNEST(JSON_EXTRACT_ARRAY(gh.payload, "$.pull_request.labels")) AS label
            ) AS pr_label
        FROM
            `githubarchive.day.20*` AS gh
        INNER JOIN
            `{project_table_id}` AS projects
        ON gh.repo.name = projects.repo
        WHERE
            _TABLE_SUFFIX BETWEEN '{start_suffix[2:]}' AND '{end_suffix[2:]}'
    )

    SELECT
        event_id,
        username,
        repository_name,
        repository_id,
        organization_name,
        organization_id,
        event_type,
        event_timestamp,
        release_id,
        JSON_VALUE(commit, "$.sha") AS commit_sha,
        JSON_VALUE(commit, "$.message") AS commit_message,
        JSON_VALUE(commit, "$.author.name") AS commit_author_name,
        JSON_VALUE(commit, "$.author.email") AS commit_author_email,
        action,
        issue_number,
        issue_title,
        issue_body,
        ARRAY_TO_STRING(issue_label, ',') AS issue_labels,
        pr_number,
        pr_title,
        pr_body,
        ARRAY_TO_STRING(pr_label, ',') AS pr_labels,
    FROM
        events
    LEFT JOIN
        UNNEST(commits) AS commit
    ORDER BY
        event_timestamp DESC
    """

    return query


def fetch_events(client: bigquery.Client, query: str) -> pd.DataFrame:
    print("Executing BigQuery query...")
    print("This may take several minutes depending on the data size...")
    
    try:
        query_job = client.query(query)
        df = query_job.to_dataframe()
        print(f"✅ Retrieved {len(df)} events")
        return df
    except Exception as e:
        print(f"❌ Error executing query: {e}")
        raise


def save_results(events_df: pd.DataFrame, output_dir: str, csv_file_name: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_csv_file = os.path.join(output_dir, csv_file_name)
    
    events_df.to_csv(output_csv_file, index=False, encoding="utf-8", quoting=1, escapechar='\\')
    print(f"✅ Saved {len(events_df)} events to: {output_csv_file}")
    
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

    #start_date = datetime(2023, 1, 1)
    #end_date = datetime(2023, 1, 2)

    start_date = datetime(2023, 2, 1)
    end_date = datetime(2023, 5, 31)

    print(f"Fetching events from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    try:
        country = "italy" # Change as needed
        out_dir = f"{Path(__file__).resolve().parent}/large_data"
        csv_file = f"events_projects_{country}.csv"

        project_table_id = f"hase-25-project.project_ratios.{country}" 

        print("Initializing BigQuery client...")
        client = bigquery.Client()
        
        query = create_bigquery_query(
            project_table_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        events_df = fetch_events(client, query) 
        
        events_file = save_results(
            events_df, 
            out_dir, 
            csv_file
        )
        
        print("\n📊 Summary:")
        print(f"  - Events found: {len(events_df)}")
        print(f"  - Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"  - Output files:")
        print(f"  - events: {events_file}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
