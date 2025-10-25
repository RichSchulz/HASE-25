#!/usr/bin/env python3
"""
Script to fetch detailed commit data from GitHub API for sampled commits.
Adds lines added/deleted information as new columns to the sample CSV files.
Respects GitHub API rate limits (5000 requests/hour).
"""

import os
import time
import pandas as pd
import requests
from pathlib import Path
from typing import Optional, Dict, Any
import argparse
from datetime import datetime, timedelta

class GitHubCommitFetcher:
    def __init__(self, token: Optional[str] = None):
        self.token = token or os.getenv("GITHUB_TOKEN")
        if not self.token:
            raise EnvironmentError(
                "GITHUB_TOKEN not set. Please set it in your environment or in a .env file."
            )
        
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "commit-sample-enricher"
        })
        
        # Rate limiting
        self.requests_made = 0
        self.rate_limit_reset = None
        self.max_requests_per_hour = 5000
        
    def check_rate_limit(self):
        """Check current rate limit status"""
        try:
            resp = self.session.get("https://api.github.com/rate_limit", timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                core = data.get("rate", {})
                remaining = core.get("remaining", 0)
                reset_time = core.get("reset", 0)
                
                print(f"Rate limit: {remaining} requests remaining, resets at {datetime.fromtimestamp(reset_time)}")
                return remaining, reset_time
        except Exception as e:
            print(f"Warning: Could not check rate limit: {e}")
            return None, None
    
    def wait_for_rate_limit_reset(self, reset_timestamp: int):
        """Wait until rate limit resets"""
        current_time = int(time.time())
        wait_seconds = max(0, reset_timestamp - current_time)
        
        if wait_seconds > 0:
            print(f"Rate limit exceeded. Waiting {wait_seconds} seconds until reset...")
            time.sleep(wait_seconds)
    
    def fetch_commit_details(self, commit_sha: str, repository_name: str) -> Optional[Dict[str, Any]]:
        """Fetch detailed commit information from GitHub API"""
        url = f"https://api.github.com/repos/{repository_name}/commits/{commit_sha}"
        
        try:
            resp = self.session.get(url, timeout=10)
            
            # Handle rate limiting
            if resp.status_code == 403:
                remaining = resp.headers.get("X-RateLimit-Remaining")
                reset = resp.headers.get("X-RateLimit-Reset")
                
                if remaining == "0" and reset:
                    reset_timestamp = int(reset)
                    self.wait_for_rate_limit_reset(reset_timestamp)
                    # Retry the request
                    resp = self.session.get(url, timeout=10)
            
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                print(f"Warning: Commit {commit_sha} not found in {repository_name}")
                return None
            else:
                print(f"Warning: Failed to fetch {commit_sha} from {repository_name}: HTTP {resp.status_code}")
                return None
                
        except Exception as e:
            print(f"Warning: Error fetching {commit_sha} from {repository_name}: {e}")
            return None
    
    def extract_commit_stats(self, commit_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant statistics from commit data"""
        stats = commit_data.get("stats", {})
        files = commit_data.get("files", [])
        
        # Calculate totals from files
        total_additions = sum(file.get("additions", 0) for file in files)
        total_deletions = sum(file.get("deletions", 0) for file in files)
        total_changes = sum(file.get("changes", 0) for file in files)
        
        return {
            "api_additions": total_additions,
            "api_deletions": total_deletions,
            "api_changes": total_changes,
            "api_files_changed": len(files),
            "api_commit_date": commit_data.get("commit", {}).get("author", {}).get("date"),
            "api_commit_message": commit_data.get("commit", {}).get("message", ""),
            "api_commit_url": commit_data.get("html_url", ""),
            "api_author_name": commit_data.get("commit", {}).get("author", {}).get("name", ""),
            "api_author_email": commit_data.get("commit", {}).get("author", {}).get("email", "")
        }

def enrich_sample_file(input_file: str, output_file: str, max_commits: Optional[int] = None):
    """Enrich a sample CSV file with GitHub API data"""
    print(f"Processing {input_file}...")
    
    # Read the sample file
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} commits from sample file")
    
    # Check required columns
    required_cols = ["repository_name", "commit_sha"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Initialize fetcher
    fetcher = GitHubCommitFetcher()
    
    # Check rate limit before starting
    remaining, reset_time = fetcher.check_rate_limit()
    if remaining is not None and remaining < 100:
        print(f"Warning: Only {remaining} API requests remaining")
    
    # Limit commits if specified
    if max_commits:
        df = df.head(max_commits)
        print(f"Limited to {max_commits} commits for processing")
    
    # Initialize new columns
    new_columns = [
        "api_additions", "api_deletions", "api_changes", "api_files_changed",
        "api_commit_date", "api_commit_message", "api_commit_url",
        "api_author_name", "api_author_email", "api_fetch_status"
    ]
    
    for col in new_columns:
        df[col] = None
    
    # Process commits
    processed = 0
    successful = 0
    failed = 0
    
    for idx, row in df.iterrows():
        processed += 1
        
        # Check rate limit every 100 requests
        if processed % 100 == 0:
            remaining, reset_time = fetcher.check_rate_limit()
            if remaining is not None and remaining < 50:
                print(f"Warning: Only {remaining} requests remaining")
        
        commit_sha = row["commit_sha"]
        repository_name = row["repository_name"]
        
        if pd.isna(commit_sha) or not isinstance(commit_sha, str) or commit_sha.strip() == "":
            df.at[idx, "api_fetch_status"] = "invalid_sha"
            failed += 1
            continue
        
        commit_sha = commit_sha.strip()
        
        print(f"Fetching {processed}/{len(df)}: {commit_sha} from {repository_name}")
        
        # Fetch commit details
        commit_data = fetcher.fetch_commit_details(commit_sha, repository_name)
        
        if commit_data:
            # Extract statistics
            stats = fetcher.extract_commit_stats(commit_data)
            
            # Update dataframe
            for key, value in stats.items():
                df.at[idx, key] = value
            
            df.at[idx, "api_fetch_status"] = "success"
            successful += 1
        else:
            df.at[idx, "api_fetch_status"] = "failed"
            failed += 1
        
        # Small delay to be respectful to the API
        time.sleep(0.1)
    
    # Save enriched data
    df.to_csv(output_file, index=False)
    
    print(f"\nProcessing complete!")
    print(f"Total processed: {processed}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {successful/processed*100:.1f}%")
    print(f"Enriched data saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Enrich commit sample files with GitHub API data")
    parser.add_argument("--input-dir", 
                       default="/Users/richard/University/HASE-25/scripts/data",
                       help="Directory containing sample CSV files")
    parser.add_argument("--output-dir", 
                       default="/Users/richard/University/HASE-25/scripts/data",
                       help="Directory to save enriched CSV files")
    parser.add_argument("--max-commits", type=int, default=None,
                       help="Maximum number of commits to process per file (for testing)")
    parser.add_argument("--country", choices=["italy", "france", "austria", "all"],
                       default="all", help="Which country's data to process")
    
    args = parser.parse_args()
    
    # Define sample files
    sample_files = {
        "italy": "commits_sample_italy_april_2023.csv",
        "france": "commits_sample_france_april_2023.csv", 
        "austria": "commits_sample_austria_april_2023.csv"
    }
    
    # Process files
    if args.country == "all":
        countries_to_process = sample_files.keys()
    else:
        countries_to_process = [args.country]
    
    for country in countries_to_process:
        input_file = os.path.join(args.input_dir, sample_files[country])
        output_file = os.path.join(args.output_dir, f"commits_sample_{country}_april_2023_enriched.csv")
        
        if not os.path.exists(input_file):
            print(f"Warning: {input_file} not found, skipping...")
            continue
        
        try:
            enrich_sample_file(input_file, output_file, args.max_commits)
            print(f"✓ Successfully processed {country}")
        except Exception as e:
            print(f"✗ Error processing {country}: {str(e)}")
        
        print("-" * 50)

if __name__ == "__main__":
    main()
