import os
import sys
import pandas as pd

INPUT_PATH = "large_data/commits_all_italy.csv"
OUTPUT_PATH = "data/italy_projects.csv"


def main():
    if not os.path.exists(INPUT_PATH):
        print(f"❌ Input file not found: {INPUT_PATH}")
        sys.exit(1)

    # Load commits
    df = pd.read_csv(INPUT_PATH)
    if df.empty:
        print("⚠️ Input CSV is empty; nothing to summarize.")
        # Ensure we still write a header-only CSV for downstream stability
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        pd.DataFrame(columns=[
            "repository_name",
            "repository_id",
            "num_commits_during_ban",
            "num_unique_users_during_ban",
        ]).to_csv(OUTPUT_PATH, index=False)
        print(f"✅ Wrote empty summary to: {OUTPUT_PATH}")
        return

    # Ensure expected columns exist
    required_cols = {"repository_name", "repository_id", "username", "event_timestamp"}
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns in input CSV: {missing}")
        sys.exit(1)

    # Parse timestamp and filter to ban window (inclusive)
    df = df.copy()
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="coerce", utc=True)
    start = pd.Timestamp("2023-04-01", tz="UTC")
    end = pd.Timestamp("2023-04-07 23:59:59", tz="UTC")
    mask = (df["event_timestamp"] >= start) & (df["event_timestamp"] <= end)
    ban_df = df.loc[mask]

    if ban_df.empty:
        print("⚠️ No commits found in the ban window (2023-04-01 to 2023-04-07).")
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        pd.DataFrame(columns=[
            "repository_name",
            "repository_id",
            "num_commits_during_ban",
            "num_unique_users_during_ban",
        ]).to_csv(OUTPUT_PATH, index=False)
        print(f"✅ Wrote empty summary to: {OUTPUT_PATH}")
        return

    grouped = (
        ban_df.groupby(["repository_id", "repository_name"], dropna=False)
        .agg(
            num_commits_during_ban=("commit_sha", "size"),
            num_unique_users_during_ban=("username", pd.Series.nunique),
        )
        .reset_index()
    )

    # Sort by distinct users desc, then commits desc
    grouped = grouped.sort_values(
        by=["num_unique_users_during_ban", "num_commits_during_ban"], ascending=[False, False]
    )

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    grouped.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Wrote {len(grouped)} projects to: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
