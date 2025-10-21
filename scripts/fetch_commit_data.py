import os
import time
from typing import Any, Optional
import requests
from dotenv import load_dotenv
import pandas as pd
import tempfile
import shutil
from pathlib import Path


# Load environment variables from .env (if present)
load_dotenv(override=True)


def fetch_commit_data(
    commit_sha: str,
    repository_full_name: str,
    token: Optional[str] = None,
    timeout: int = 10,
) -> dict[str, Any]:
    # Use provided token or read from environment
    token = token or os.getenv("GITHUB_TOKEN")
    if not token:
        raise EnvironmentError(
            "GITHUB_TOKEN not set. Please set it in your environment or in a .env file."
        )

    # Expect repository_full_name to be "owner/repo"
    if "/" not in repository_full_name:
        raise ValueError(
            'repository_full_name must be in the form "owner/repo" (e.g. "octocat/Hello-World")'
        )

    url = f"https://api.github.com/repos/{repository_full_name}/commits/{commit_sha}"

    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "gh-archive-bigquery-fetcher",
    }

    resp = requests.get(url, headers=headers, timeout=timeout)

    # Success
    if resp.status_code == 200:
        return resp.json()

    # Not found
    if resp.status_code == 404:
        raise ValueError(
            f"Commit '{commit_sha}' or repository '{repository_full_name}' not found (HTTP 404)."
        )

    # Authentication or rate limit issues
    if resp.status_code in (401, 403):
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining == "0" and reset is not None:
            try:
                reset_ts = int(reset)
                wait_seconds = max(0, reset_ts - int(time.time()))
                raise RuntimeError(
                    f"Rate limit exceeded. X-RateLimit-Remaining=0. "
                    f"Rate limit resets in {wait_seconds} seconds (at unix {reset_ts})."
                )
            except ValueError:
                # header not integer for some reason
                raise RuntimeError("Rate limit exceeded (403).")

        # Otherwise, include response body for debugging
        raise RuntimeError(
            f"Authentication or permission error (HTTP {resp.status_code}): {resp.text}"
        )

    # Other HTTP error
    raise RuntimeError(
        f"GitHub API request failed (HTTP {resp.status_code}): {resp.text}"
    )


def commit_json_to_rows(commit_row: Any, commit_json: dict) -> list[dict]:
    rows = []

    username = commit_row.username
    repsitory_name = commit_row.repository_name
    commit_sha = commit_row.commit_sha
    commit_message = commit_row.commit_message
    push_event_timestamp = commit_row.event_timestamp
    files = commit_json.get("files") or []

    for file in files:
        row = {}
        row["repository_name"] = repsitory_name
        row["username"] = username
        row["commit_sha"] = commit_sha
        row["commit_message"] = commit_message
        row["push_event_timestamp"] = push_event_timestamp
        row["filename"] = file.get("filename")
        row["status"] = file.get("status")
        row["additions"] = file.get("additions")
        row["deletions"] = file.get("deletions")
        row["changes"] = file.get("changes")
        row["patch"] = file.get("patch")

        rows.append(row)

    return rows


def get_top_repository_name(idx: int, projects_csv: str) -> str:
    # Read top projects
    if not os.path.exists(projects_csv):
        raise FileNotFoundError(f"projects_csv not found: {projects_csv}")

    projects_df = pd.read_csv(projects_csv)
    if "repository_name" not in projects_df.columns:
        raise ValueError("projects_csv must contain a 'repository_name' column")

    top_repos = projects_df.iloc[idx]["repository_name"]
    return top_repos


def fetch_commits_and_update_csv(
    repository_name: str,
    commits_csv: str,
    output_dir: str,
    chunksize: int = 10000,
    commits_limit: Optional[int] = None,
):
    commits_path = Path(commits_csv)
    if not commits_path.exists():
        raise FileNotFoundError(f"commits CSV not found: {commits_csv}")

    csv_file_name = f"commit_changes__{repository_name.replace('/', '--')}.csv"
    os.makedirs(output_dir, exist_ok=True)
    output_path = Path(os.path.join(output_dir, csv_file_name))

    tmpf = tempfile.NamedTemporaryFile(delete=False, prefix="tmp_commit_changes__", suffix=".csv", dir=str(output_path.parent))
    tmpf_name = tmpf.name
    tmpf.close()

    first_write = True

    def limit_reached() -> bool:
        return commits_limit is not None and commits_idx >= commits_limit

    def ensure_columns(df: pd.DataFrame, cols: list):
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA

    col_names = [
        "repository_name",
        "username",
        "commit_sha",
        "commit_message",
        "push_event_timestamp",
        "filename",
        "status",
        "additions",
        "deletions",
        "changes",
        "patch",
    ]

    commits_idx = 0

    reader = pd.read_csv(commits_csv, dtype=str, chunksize=chunksize)
    chunk_idx = 0
    for chunk in reader:
        if limit_reached():
            break

        chunk_idx += 1
        chunk = chunk.copy()

        # Ensure expected columns exist locally
        if "repository_name" not in chunk.columns or "commit_sha" not in chunk.columns:
            raise ValueError("commits CSV must contain 'repository_name' and 'commit_sha' columns")

        ensure_columns(chunk, col_names)

        # Find rows that belong to top repos
        repsitory_commits = chunk[chunk["repository_name"] == repository_name]

        write_cache: list[dict[str, Any]] = []

        # For each unique commit_sha in this subset, fetch if we haven't yet
        for row in repsitory_commits.itertuples(index=False):
            if limit_reached():
                break

            commit_sha = row.commit_sha
            if pd.isna(commit_sha) or not isinstance(commit_sha, str) or commit_sha.strip() == "":
                continue

            commit_sha = commit_sha.strip()

            try:
                commits_idx += 1
                print(f"Fetching {commit_sha} from {repository_name} (commit {commits_idx}, chunk {chunk_idx})")
                commit_json = fetch_commit_data(commit_sha, repository_name)
                file_rows = commit_json_to_rows(row, commit_json)
                write_cache.extend(file_rows)

            except Exception as e:
                # Log and continue; put a placeholder so we don't retry repeatedly
                print(f"Warning: failed to fetch {commit_sha} from {repository_name}: {e}")

        row_df = pd.DataFrame(write_cache, columns=col_names)
        if first_write:
            row_df.to_csv(tmpf_name, index=False, mode="w", encoding="utf-8")
            first_write = False
        else:
            row_df.to_csv(tmpf_name, index=False, mode="a", header=False, encoding="utf-8")

    # Move temp file to the output path (do not overwrite original commits_csv)
    shutil.move(tmpf_name, str(output_path))
    print(f"Wrote commit changes to CSV to: {output_path}")


def main():
    load_dotenv(override=True)

    # top_projects = [get_top_repository_name(i, "data/italy_projects_fulltime.csv") for i in range(10)]

    # for project in top_projects:
    #     fetch_commits_and_update_csv(
    #         repository_name=project,
    #         commits_csv="large_data/commits_all_italy.csv",
    #         output_dir="large_data",
    #         # commits_limit=5 # set this for testing
    #     )

    fetch_commits_and_update_csv(
        repository_name="pagopa/io-app",
        commits_csv="large_data/commits_all_italy.csv",
        output_dir="large_data",
        # commits_limit=5 # set this for testing
    )


if __name__ == "__main__":
    main()
