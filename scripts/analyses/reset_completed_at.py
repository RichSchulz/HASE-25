"""
Reset completed_at in download progress for all commits.
"""

import sqlite3
from pathlib import Path


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
    if not confirm_action("Are you sure you want to reset the completed_at column for all entries?"):
        print("👋 bye")
        return

    conn = sqlite3.connect(f"{Path(__file__).resolve().parent.parent}/large_data/commits_all.sqlite3")
    cur = conn.cursor()
    cur.execute(
        f"""UPDATE commits SET completed_at = NULL
        """
    )
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
