from dataclasses import dataclass
import os
import time
from typing import Any, Optional
import requests
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import threading
import time
from queue import Queue


class CommitNotFoundError(Exception):
    pass


@dataclass(frozen=True)
class CommitResult:
    job_ids: list[int]
    file_rows: list[dict]
    commit_rows: list[dict]

@dataclass(frozen=True)
class JobRequest:
    pass

@dataclass(frozen=True)
class JobResult:
    rows: list[dict]
    started: int
    total: int


def create_data_files_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
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
    cur = conn.cursor()
    cur.execute(
        f"""CREATE TABLE IF NOT EXISTS commit_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country TEXT,
            repository_name TEXT,
            username TEXT,
            commit_sha TEXT,
            commit_message TEXT,
            push_event_timestamp TIMESTAMP,
            files INTEGER,
            additions INTEGER,
            deletions INTEGER,
            changes INTEGER
        )"""
    )
    conn.commit()


def reset_incomplete_downloads(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        f"""UPDATE commits SET started_at = NULL WHERE completed_at IS NULL
        """
    )
    conn.commit()


def fetch_commit_data(
    commit_sha: str,
    repository_full_name: str,
    token: str,
    timeout: int = 10,
) -> dict[str, Any] | int:
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
    if resp.status_code in (404, 409, 422, 451,):
        raise CommitNotFoundError(
            f"Error loading commit '{commit_sha}' at '{repository_full_name}' (HTTP {resp.status_code}): {resp.text}."
        )

    # Authentication or rate limit issues
    if resp.status_code in (401, 403):
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining == "0" and reset is not None:
            try:
                reset_ts = int(reset)
                return reset_ts
            except ValueError:
                # header not integer for some reason
                raise RuntimeError("Rate limit exceeded (403).")

        # Otherwise, include response body for debugging
        raise CommitNotFoundError(
            f"Error loading commit '{commit_sha}' at '{repository_full_name}' (HTTP {resp.status_code}): {resp.text}."
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


def read_job_row(cur: sqlite3.Cursor, limit: int) -> list[dict]:
    cur.execute(f"SELECT * FROM commits WHERE started_at IS NULL LIMIT {limit}")
    return cur.fetchall()

def read_job_counts(cur: sqlite3.Cursor) -> tuple[int, int]:
    cur.execute(
        """SELECT
            COUNT(*) AS total_count,
            SUM(CASE WHEN started_at IS NOT NULL THEN 1 ELSE 0 END) AS non_null_started_at_count
        FROM commits"""
    )
    counts = cur.fetchone()
    return (counts["non_null_started_at_count"], counts["total_count"])

def write_jobs_started(cur: sqlite3.Cursor, ids: list[int]):
    if not ids:
        return

    in_ids = ", ".join("?" for _ in ids)
    cur.execute(f"UPDATE commits SET started_at = CURRENT_TIMESTAMP WHERE id IN ({in_ids})", ids)


def write_jobs_completed(cur: sqlite3.Cursor, ids: list[int]):
    if not ids:
        return

    in_ids = ", ".join("?" for _ in ids)
    cur.execute(f"UPDATE commits SET completed_at = CURRENT_TIMESTAMP WHERE id IN ({in_ids})", ids)


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


def write_commit_rows(cur: sqlite3.Cursor, commit_rows: list[dict]):
    cur.executemany(
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
        commit_rows
    )


def db_worker(request_queue: Queue, response_queue: Queue):
    job_conn = sqlite3.connect("large_data/commits_all.sqlite3", check_same_thread=False)
    job_conn.row_factory = sqlite3.Row

    files_conn = sqlite3.connect("large_data/data_files.sqlite3", check_same_thread=False)
    commits_conn = sqlite3.connect("large_data/data_commits.sqlite3", check_same_thread=False)

    create_data_files_table(files_conn)
    create_data_commits_table(commits_conn)

    # Reset downloads from a previous run (started_at set but completed_at not set)
    reset_incomplete_downloads(job_conn)

    while True:
        req = request_queue.get()

        if isinstance(req, JobRequest):
            try:
                cur = job_conn.cursor()
                job_rows = read_job_row(cur, limit=50)

                if not job_rows:
                    print(f"[db_worker]: No more pending jobs found in database")
                    response_queue.put(None)
                    continue

                job_ids = [row["id"] for row in job_rows]
                write_jobs_started(cur, ids=job_ids)
                job_conn.commit()

                started_count, total_count = read_job_counts(cur)

                response_queue.put(JobResult(rows=job_rows, started=started_count, total=total_count))

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
                write_commit_rows(commits_cur, req.commit_rows)
                commits_conn.commit()

                cur = job_conn.cursor()
                write_jobs_completed(cur, req.job_ids)
                job_conn.commit()

            except Exception as e:
                print(f"[db_worker]: Error in db_worker when writing commit result:", e)
                continue

            finally:
                request_queue.task_done()

        else:
            request_queue.task_done()
            if request_queue.empty():
                print(f"[db_worker]: Terminating")
                break


    files_conn.close()
    commits_conn.close()
    job_conn.close()


def download_worker(request_queue: Queue, response_queue: Queue, stop_event: threading.Event, thread_id: int, token: str):
    while not stop_event.is_set():
        request_queue.put(JobRequest())
        job_result = response_queue.get()

        if not isinstance(job_result, JobResult):
            print(f"[download_worker-{thread_id}]: Terminating")
            response_queue.task_done()
            break

        job_rows = job_result.rows
        job_rows_count = len(job_rows)

        job_ids: list[int] = []
        file_rows: list[dict] = []
        commit_rows: list[dict] = []

        for job_idx, job_row in enumerate(job_rows):
            if stop_event.is_set():
                break

            job_id = job_row["id"]

            try:
                commit_sha = job_row['commit_sha']
                repository_name = job_row['repository_name']

                # job-result.started is the count of all jobs in this batch, so we need
                # to calculate the how many jobs have actually been downloaded
                started_count = job_result.started-job_rows_count+job_idx+1
                
                print(f"[download_worker-{thread_id}]: Downloading ({started_count}/{job_result.total}) {commit_sha} from {repository_name}")

                # Handle waiting when rate limiting is reached in a way that allows stopping if stop_event is set
                continue_at = 0
                while not stop_event.is_set():
                    current_time = time.time()
                    if current_time > continue_at+1:
                        fetch_result = fetch_commit_data(
                            commit_sha=commit_sha,
                            repository_full_name=repository_name,
                            token=token
                        )

                        if isinstance(fetch_result, int):
                            continue_at = fetch_result
                            wait_seconds = max(0, continue_at - int(current_time))
                            print(f"[download_worker-{thread_id}]: Time limit reached, waiting for {wait_seconds}s")
                        else:
                            commit_data = fetch_result
                            job_ids.append(job_id)
                            file_rows.extend(commit_json_to_file_rows(job_row, commit_data))
                            commit_rows.append(commit_json_to_commit_row(job_row, commit_data))
                            break
                    
                    time.sleep(0.5)


            except CommitNotFoundError as e:
                print(f"[download_worker-{thread_id}]: Commit not found, still marking as completed:", e)
                job_ids.append(job_id)

            except Exception as e:
                print(f"[download_worker-{thread_id}]: Error in download_worker when fetching commit data:", e)

        if stop_event.is_set():
            print(f"[download_worker-{thread_id}]: Terminating (stop_event)")

        request_queue.put(CommitResult(job_ids=job_ids, file_rows=file_rows, commit_rows=commit_rows)) 
        response_queue.task_done()


def get_github_tokens():
    tokens: list[str] = []
    token_id = 1
    while True:
        token_name = f"GITHUB_TOKEN_{token_id}"
        token = os.getenv(token_name)
        if token:
            tokens.append(token)
            token_id += 1
        else:
            if tokens:
                break
            else:
                raise EnvironmentError(
                    f"{token_name} not set. Specify at least one github token in you .env file as GITHUB_TOKEN_1"
                )

    return tokens


def main():
    load_dotenv(override=True)

    threads_per_token = 2

    stop_event = threading.Event()
    request_queue = Queue()
    response_queue = Queue()

    threads: list[threading.Thread] = []

    db_thread = threading.Thread(target=db_worker, kwargs={
        'request_queue': request_queue,
        'response_queue': response_queue
    })
    db_thread.start()

    tokens = get_github_tokens()
    num_tokens = len(tokens)
    num_threads = num_tokens * threads_per_token

    for i in range(num_threads):
        thread_id = i+1
        token = tokens[i%num_tokens]

        t = threading.Thread(target=download_worker, kwargs={
            'request_queue': request_queue,
            'response_queue': response_queue,
            'stop_event': stop_event,
            'thread_id': thread_id,
            'token': token
        })
        t.start()
        threads.append(t)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    
    except KeyboardInterrupt:
        print("\nCtrl+C detected. Stopping downloads...")
        stop_event.set()

    finally:
        # Wait for all downloads to finish
        print("Waiting for download threads to finish")
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
