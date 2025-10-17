import os
import time
from typing import Any, Dict, Optional

import requests
from dotenv import load_dotenv
import pandas as pd


# Load environment variables from .env (if present)
load_dotenv(override=True)


def fetch_commit_data(
    commit_sha: str,
    repository_full_name: str,
    token: Optional[str] = None,
    timeout: int = 10,
) -> Dict[str, Any]:
    """Fetch a commit object from the GitHub REST API.

    Args:
        commit_sha: The commit SHA to fetch (full or short SHA).
        repository_full_name: Repository full name in the form "owner/repo".
        token: Optional GitHub personal access token. If not provided, the
            function will read from the environment variable `GITHUB_TOKEN`.
        timeout: Request timeout in seconds.

    Returns:
        A dict parsed from the GitHub API JSON response for the commit.

    Raises:
        EnvironmentError: If no token is provided or available in the env.
        ValueError: If the commit or repository was not found (HTTP 404).
        RuntimeError: For authentication, rate-limit, or other HTTP errors.
    """

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

    print(f"data: {resp.text}")

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

def commit_json_to_row(commit_json: dict) -> dict:
    """Convert GitHub commit JSON to a flat dict suitable for CSV.

    The function extracts commonly useful fields:
    - repository full name (if available under 'url' or parent)
    - sha, html_url, message, author name/email/date, committer name/email/date,
      stats (additions/deletions/total if present), files changed (names joined with ';')
    """

    row = {}

    # Commit level
    row["sha"] = commit_json.get("sha")
    row["html_url"] = commit_json.get("html_url")

    # Commit message
    commit_obj = commit_json.get("commit", {})
    row["message"] = commit_obj.get("message")

    # Author
    author = commit_obj.get("author") or {}
    row["author_name"] = author.get("name")
    row["author_email"] = author.get("email")
    row["author_date"] = author.get("date")

    # Committer
    committer = commit_obj.get("committer") or {}
    row["committer_name"] = committer.get("name")
    row["committer_email"] = committer.get("email")
    row["committer_date"] = committer.get("date")

    # Stats (may be absent for some responses)
    stats = commit_json.get("stats") or {}
    row["additions"] = stats.get("additions")
    row["deletions"] = stats.get("deletions")
    row["total_changes"] = stats.get("total")

    # Files changed: store filenames joined by semicolon
    files = commit_json.get("files") or []
    try:
        filenames = [f.get("filename") for f in files if f.get("filename")]
    except Exception:
        filenames = []
    row["files_changed"] = ";".join(filenames)

    # Repository context: sometimes the response includes a 'repository' object
    repo = commit_json.get("repository")
    if repo and isinstance(repo, dict):
        row["repository_full_name"] = repo.get("full_name") or repo.get("name")
    else:
        row["repository_full_name"] = None

    return row


# TODO: Change these values
commit_sha = "5d55c58aac3530658cd86abf461cdfe32a2388c3"
repository_full_name = "vulcanoFe/learning-react"

data = fetch_commit_data(commit_sha, repository_full_name)
row = commit_json_to_row(data)

out_csv = "github_commits.csv"
# Use pandas to create a single-row DataFrame and write CSV
df = pd.DataFrame([row])
df.to_csv(out_csv, index=False, encoding="utf-8")
print(f"Saved flattened commit CSV to {out_csv}")
