from dataclasses import dataclass
import os
import time
from typing import Any, Optional
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import threading
from queue import Queue


class CommitNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class CommitResult:
    job_id: str
    file_rows: list[dict]
    commit_row: dict

@dataclass(frozen=True)
class CommitNotFound:
    job_id: str

@dataclass(frozen=True)
class RequestJob:
    pass

request_queue = Queue()
response_queue = Queue()

def create_data_files_table(conn: sqlite3.Connection):
    conn.cursor()
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS file_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT,
            repository_name TEXT,
            username TEXT,
            commit_sha TEXT,
            commit_message TEXT,
            push_event_timestamp TIMESTAMP,
            filename TEXT,
            status TEXT,
            additions INTEGER,
            deletions INTEGER,
            changes INTEGER,
            patch TEXT
        )"""
    )
    conn.commit()


def create_data_commits_table(conn: sqlite3.Connection):
    conn.cursor()
    conn.execute(
        f"""CREATE TABLE IF NOT EXISTS commit_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT,
            repository_name TEXT,
            username TEXT,
            commit_sha TEXT,
            commit_message TEXT,
            push_event_timestamp TIMESTAMP,
            FILES INTEGER,
            additions INTEGER,
            deletions INTEGER,
            changes INTEGER
        )"""
    )
    conn.commit()


def fetch_commit_data(
    commit_sha: str,
    repository_full_name: str,
    token: str,
    timeout: int = 10,
) -> dict[str, Any]:
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
        raise CommitNotFoundError(
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


def commit_json_to_file_rows(commit_row: Any, commit_json: dict) -> list[dict]:
    rows = []

    country = commit_row["country"]
    username = commit_row["username"]
    repsitory_name = commit_row["repository_name"]
    commit_sha = commit_row["commit_sha"]
    commit_message = commit_row["commit_message"]
    push_event_timestamp = commit_row["event_timestamp"]
    files = commit_json.get("files") or []

    for file in files:
        row = {}
        row["country"] = country
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


def commit_json_to_commit_row(commit_row: Any, commit_json: dict) -> dict:
    country = commit_row["country"]
    username = commit_row["username"]
    repsitory_name = commit_row["repository_name"]
    commit_sha = commit_row["commit_sha"]
    commit_message = commit_row["commit_message"]
    push_event_timestamp = commit_row["event_timestamp"]
    files = commit_json.get("files") or []
    additions = commit_json.get("stats", {}).get("additions")
    deletions = commit_json.get("stats", {}).get("deletions")
    changes = commit_json.get("stats", {}).get("total")

    return {
        'country': country,
        'repository_name': repsitory_name,
        'username': username,
        'commit_sha': commit_sha,
        'commit_message': commit_message,
        'push_event_timestamp': push_event_timestamp,
        'files': len(files),
        'additions': additions,
        'deletions': deletions,
        'changes': changes,
    }


def get_top_repository_name(idx: int, projects_csv: str) -> str:
    # Read top projects
    if not os.path.exists(projects_csv):
        raise FileNotFoundError(f"projects_csv not found: {projects_csv}")

    projects_df = pd.read_csv(projects_csv)
    if "repository_name" not in projects_df.columns:
        raise ValueError("projects_csv must contain a 'repository_name' column")

    top_repos = projects_df.iloc[idx]["repository_name"]
    return top_repos


def write_job_started(cur: sqlite3.Cursor, id: int):
    cur.execute(f"UPDATE commits SET started_at = CURRENT_TIMESTAMP WHERE id = ?", (id,))


def write_job_completed(cur: sqlite3.Cursor, id: str):
    cur.execute(f"UPDATE commits SET completed_at = CURRENT_TIMESTAMP WHERE id = ?", (id,))


def write_file_rows(cur: sqlite3.Cursor, file_rows: list[dict]):
    cur.executemany(
        f"""INSERT INTO file_data (
            country,
            repository_name,
            username,
            commit_sha,
            commit_message,
            push_event_timestamp,
            filename,
            status,
            additions,
            deletions,
            changes,
            patch
        ) VALUES (
            :country,
            :repository_name,
            :username,
            :commit_sha,
            :commit_message,
            :push_event_timestamp,
            :filename,
            :status,
            :additions,
            :deletions,
            :changes,
            :patch
        )""",
        file_rows
    )


def write_commit_row(cur: sqlite3.Cursor, commit_row: dict):
    cur.execute(
        f"""INSERT INTO commit_data (
            country,
            repository_name,
            username,
            commit_sha,
            commit_message,
            push_event_timestamp,
            files,
            additions,
            deletions,
            changes
        ) VALUES (
            :country,
            :repository_name,
            :username,
            :commit_sha,
            :commit_message,
            :push_event_timestamp,
            :files,
            :additions,
            :deletions,
            :changes
        )""",
        commit_row
    )


def db_worker():
    job_conn = sqlite3.connect("large_data/commits_all_sample.sqlite3", check_same_thread=False)
    job_conn.row_factory = sqlite3.Row

    files_conn = sqlite3.connect("large_data/data_files.sqlite3", check_same_thread=False)
    commits_conn = sqlite3.connect("large_data/data_commits.sqlite3", check_same_thread=False)

    create_data_files_table(files_conn)
    create_data_commits_table(commits_conn)

    while True:
        req = request_queue.get()

        if isinstance(req, RequestJob):
            try: 
                cur = job_conn.cursor()
                cur.execute(f"SELECT * FROM commits WHERE started_at IS NULL LIMIT 1")
                job_row = cur.fetchone()

                if job_row is None:
                    print(f"[db_worker]: No more pending jobs found in database")
                    job_conn.commit()
                    response_queue.put(None)
                    continue

                write_job_started(cur, job_row["id"])
                job_conn.commit()
                response_queue.put(job_row)

            except Exception as e:
                print(f"[db_worker]: Error in db_worker when reading jobs:", e)
                response_queue.put(None)
                continue

            finally:
                request_queue.task_done()

        elif isinstance(req, CommitResult):
            try:
                files_cur = files_conn.cursor()
                write_file_rows(files_cur, req.file_rows)
                files_conn.commit()

                commits_cur = commits_conn.cursor()
                write_commit_row(commits_cur, req.commit_row)
                commits_conn.commit()

                cur = job_conn.cursor()
                write_job_completed(cur, req.job_id)
                job_conn.commit()

            except Exception as e:
                print(f"[db_worker]: Error in db_worker when writing commit result:", e)
                continue

            finally:
                request_queue.task_done()

        elif isinstance(req, CommitNotFound):
            try:
                cur = job_conn.cursor()
                write_job_completed(cur, req.job_id)
                job_conn.commit()

            except Exception as e:
                print(f"[db_worker]: Error in db_worker when writing commit not found result:", e)
                continue

            finally:
                request_queue.task_done()

        else:
            print(f"[db_worker]: Terminating")
            request_queue.task_done()
            break


    files_conn.close()
    commits_conn.close()
    job_conn.close()


def download_worker(idx: int, token: str):
    while True:
        request_queue.put(RequestJob())
        job_row = response_queue.get()

        if job_row is None:
            print(f"[download_worker {idx}]: Terminating")
            response_queue.task_done()
            break

        try:
            commit_sha = job_row['commit_sha']
            repository_name = job_row['repository_name']

            print(f"[download_worker {idx}]: Downloading {commit_sha} from {repository_name}")

            commit_data = fetch_commit_data(
                commit_sha=commit_sha,
                repository_full_name=repository_name,
                token=token
            )

            file_rows = commit_json_to_file_rows(job_row, commit_data)
            commit_row = commit_json_to_commit_row(job_row, commit_data)

            request_queue.put(CommitResult(job_id=job_row['id'], file_rows=file_rows, commit_row=commit_row))

        except CommitNotFoundError as e:
            print(f"[download_worker {idx}]: Commit not found, still marking as completed:", e)
            request_queue.put(CommitNotFound(job_id=job_row['id']))
            continue

        except Exception as e:
            print(f"[download_worker {idx}]: Error in download_worker when fetching commit data:", e)
            continue

        finally:
            response_queue.task_done()


def main():
    load_dotenv(override=True)

    num_threads = 4
    threads: list[threading.Thread] = []

    db_thread = threading.Thread(target=db_worker)
    db_thread.start()

    for i in range(num_threads):
        idx = i+1

        token_name = f"GITHUB_TOKEN_{idx}"
        token = os.getenv(token_name)
        if not token:
            raise EnvironmentError(
                f"{token_name} not set. Please set it in your environment or in a .env file."
            )

        t = threading.Thread(target=download_worker, args=(idx,token,))
        t.start()
        threads.append(t)

    # Wait for all downloads to finish
    print("Waiting for downloads threads to finish")
    for t in threads:
        t.join()

    # Wait for all downloads to be written
    print("Waiting for response_queue to finish")
    response_queue.join()

    # Signal that all downloads have finished 
    request_queue.put(None)

    # Wait for all requests to finish
    print("Waiting for request_queue to finish")
    request_queue.join()

    # Wait for db worker thread to finish
    print("Waiting for db_thread to finish")
    db_thread.join()


if __name__ == "__main__":
    main()
