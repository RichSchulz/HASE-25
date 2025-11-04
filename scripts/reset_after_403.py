import sqlite3


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
    if not confirm_action("Are you sure you want cleanup entries?"):
        print("👋 bye")
        return
    
    cutoff = "2025-11-03 10:03:00"

    jobs_conn = sqlite3.connect("large_data/commits_all.sqlite3")
    jobs_cur = jobs_conn.cursor()
    jobs_cur.execute(
        f"""UPDATE commits
            SET started_at = NULL, completed_at = NULL
            WHERE started_at > ?""",
        (cutoff,)
    )
    jobs_conn.commit()

    jobs_cur.execute(
        f"""SELECT repository_name, commit_sha
            FROM commits
            WHERE completed_at IS NOT NULL
            ORDER BY completed_at DESC
            LIMIT 1
        """
    )
    commits_repository_name, commits_commit_sha = jobs_cur.fetchone()
    jobs_conn.close()

    commits_conn = sqlite3.connect("large_data/data_commits.sqlite3")
    commits_cur = commits_conn.cursor()

    commits_cur.execute(
        f"""SELECT id FROM commit_data
            WHERE repository_name = ? AND commit_sha = ?
            ORDER BY id DESC
            LIMIT 1
        """,
        (commits_repository_name,commits_commit_sha,)
    )
    commits_last_id, = commits_cur.fetchone()

    commits_cur.execute(
        f"""DELETE FROM commit_data
            WHERE id > ?
        """,
        (commits_last_id,)
    )
    commits_conn.commit()

    commits_cur.execute(
        f"""UPDATE sqlite_sequence
            SET seq = (SELECT MAX(id) FROM commit_data)
            WHERE name = 'commit_data'
        """
    )
    commits_conn.commit()

    commits_cur.execute(
        f"""SELECT repository_name, commit_sha
            FROM commit_data
            WHERE files > 0
            ORDER BY id DESC
            LIMIT 1
        """
    )
    files_repository_name, files_commit_sha = commits_cur.fetchone()

    commits_conn.close()

    files_conn = sqlite3.connect("large_data/data_files.sqlite3")
    files_cur = files_conn.cursor()

    files_cur.execute(
        f"""SELECT id FROM file_data
            WHERE repository_name = ? AND commit_sha = ?
            ORDER BY id DESC
            LIMIT 1
        """,
        (files_repository_name,files_commit_sha,)
    )
    files_last_id, = files_cur.fetchone()

    files_cur.execute(
        f"""DELETE FROM file_data
            WHERE id > ?
        """,
        (files_last_id,)
    )
    files_conn.commit()

    files_cur.execute(
        f"""UPDATE sqlite_sequence
            SET seq = (SELECT MAX(id) FROM file_data)
            WHERE name = 'file_data'
        """
    )
    files_conn.commit()
    files_conn.close()


if __name__ == "__main__":
    main()
