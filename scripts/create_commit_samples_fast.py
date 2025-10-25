#!/usr/bin/env python3
"""
Optimized script to create representative samples from commit events CSV files.
Focuses on commits around April 1st, 2023 to compare activity before and after this date.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
import os
import argparse
from pathlib import Path

def create_representative_sample_optimized(input_file, output_file, sample_size=10000, random_seed=42):
    """
    Create a representative sample from commit events CSV file - OPTIMIZED VERSION.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        sample_size: Number of commits to sample (default: 10,000)
        random_seed: Random seed for reproducibility
    """
    print(f"Processing {input_file}...")
    
    # Define date ranges as strings for faster filtering
    before_start = "2023-03-18"  # 14 days before April 1st
    april_1st = "2023-04-01"
    april_14th = "2023-04-15"    # 14 days after April 1st (April 1st + 14 days = April 15th)
    
    # Use pandas' efficient string filtering first, then convert to datetime only for final filtering
    print("Reading and filtering data...")
    
    # Read CSV with only the columns we need
    columns_to_use = ['username', 'repository_name', 'repository_id', 'organization_name', 
                     'organization_id', 'event_timestamp', 'branch_name', 'commit_sha', 
                     'commit_message', 'commit_author_name', 'commit_author_email', 
                     'push_size', 'distinct_commits', 'head_commit_sha', 'before_commit_sha']
    
    # First pass: filter by date strings (much faster than datetime conversion)
    df_filtered = pd.read_csv(
        input_file,
        usecols=columns_to_use,
        chunksize=50000  # Process in smaller chunks
    )
    
    # Collect chunks that fall within our date range
    # Since files are sorted newest first, we need to read more chunks to get older data
    relevant_chunks = []
    total_rows = 0
    chunks_processed = 0
    max_chunks = 200  # Process up to 200 chunks (10M rows) to ensure we get both periods
    
    for chunk in df_filtered:
        chunks_processed += 1
        # Convert timestamps and filter by date range
        chunk['event_timestamp'] = pd.to_datetime(chunk['event_timestamp'].str.strip('"'), utc=True)
        
        # Quick string-based pre-filtering
        chunk['date_str'] = chunk['event_timestamp'].dt.strftime('%Y-%m-%d')
        mask = (chunk['date_str'] >= before_start) & (chunk['date_str'] < april_14th)
        filtered_chunk = chunk[mask].drop('date_str', axis=1)
        
        if len(filtered_chunk) > 0:
            relevant_chunks.append(filtered_chunk)
            total_rows += len(filtered_chunk)
        
        # Stop if we've processed enough chunks (ensure we get both periods)
        if chunks_processed >= max_chunks:
            break
    
    print(f"Processed {chunks_processed} chunks, found {total_rows} relevant commits")
    
    if not relevant_chunks:
        print("No commits found in target date range!")
        return
    
    # Combine chunks
    df = pd.concat(relevant_chunks, ignore_index=True)
    print(f"Found {len(df)} commits in target date range (Mar 18 - Apr 14, 2023)")
    
    # Split into before and after April 1st using efficient datetime operations
    april_1st_ts = pd.Timestamp('2023-04-01', tz='UTC')
    
    before_mask = df['event_timestamp'] < april_1st_ts
    after_mask = df['event_timestamp'] >= april_1st_ts
    
    df_before = df[before_mask]
    df_after = df[after_mask]
    
    print(f"Before April 1st: {len(df_before)} commits")
    print(f"After April 1st: {len(df_after)} commits")
    
    # Debug: show some sample timestamps
    if len(df_before) > 0:
        print(f"Sample before timestamps: {df_before['event_timestamp'].head(3).tolist()}")
    if len(df_after) > 0:
        print(f"Sample after timestamps: {df_after['event_timestamp'].head(3).tolist()}")
    
    # Calculate proportional sampling
    total_relevant = len(df_before) + len(df_after)
    if total_relevant == 0:
        print("No relevant commits found!")
        return
    
    # Ensure we have enough commits from both periods for representative sampling
    min_per_period = max(50, sample_size // 20)  # At least 50 or 5% of sample
    
    if len(df_before) < min_per_period or len(df_after) < min_per_period:
        print(f"Warning: Not enough commits in one or both periods for representative sampling")
        print(f"Using all available commits from both periods (total: {total_relevant})")
        sample_before = df_before
        sample_after = df_after
    else:
        # Proportional sampling
        before_ratio = len(df_before) / total_relevant
        after_ratio = len(df_after) / total_relevant
        
        sample_before_size = min(len(df_before), int(sample_size * before_ratio))
        sample_after_size = min(len(df_after), int(sample_size * after_ratio))
        
        # Adjust if we need more samples
        if sample_before_size + sample_after_size < sample_size:
            remaining = sample_size - (sample_before_size + sample_after_size)
            if len(df_before) > sample_before_size:
                sample_before_size += min(remaining, len(df_before) - sample_before_size)
            elif len(df_after) > sample_after_size:
                sample_after_size += min(remaining, len(df_after) - sample_after_size)
        
        # Sample from each period
        np.random.seed(random_seed)
        sample_before = df_before.sample(n=sample_before_size, random_state=random_seed)
        sample_after = df_after.sample(n=sample_after_size, random_state=random_seed)
    
    # Combine samples
    final_sample = pd.concat([sample_before, sample_after], ignore_index=True)
    
    # Shuffle the final sample
    final_sample = final_sample.sample(frac=1, random_state=random_seed).reset_index(drop=True)
    
    print(f"Created sample with {len(final_sample)} commits")
    print(f"  - Before April 1st: {len(sample_before)} commits")
    print(f"  - After April 1st: {len(sample_after)} commits")
    
    # Save to CSV
    final_sample.to_csv(output_file, index=False)
    print(f"Sample saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Create representative samples from commit events CSV files (OPTIMIZED)')
    parser.add_argument('--input-dir', default='/Users/richard/University/HASE-25/scripts/data',
                       help='Directory containing input CSV files')
    parser.add_argument('--output-dir', default='/Users/richard/University/HASE-25/scripts/data',
                       help='Directory to save output CSV files')
    parser.add_argument('--sample-size', type=int, default=10000,
                       help='Number of commits to sample (default: 10000)')
    parser.add_argument('--random-seed', type=int, default=42,
                       help='Random seed for reproducibility (default: 42)')
    
    args = parser.parse_args()
    
    # Define input files
    input_files = {
        'italy': os.path.join(args.input_dir, 'commits_all_italy_commit_events.csv'),
        'france': os.path.join(args.input_dir, 'commits_all_france_commit_events.csv'),
        'austria': os.path.join(args.input_dir, 'commits_all_austria_commit_events.csv')
    }
    
    # Process each file
    for country, input_file in input_files.items():
        if not os.path.exists(input_file):
            print(f"Warning: {input_file} not found, skipping...")
            continue
        
        output_file = os.path.join(args.output_dir, f'commits_sample_{country}_april_2023.csv')
        
        try:
            create_representative_sample_optimized(
                input_file=input_file,
                output_file=output_file,
                sample_size=args.sample_size,
                random_seed=args.random_seed
            )
            print(f"✓ Successfully processed {country}")
        except Exception as e:
            print(f"✗ Error processing {country}: {str(e)}")
        
        print("-" * 50)

if __name__ == "__main__":
    main()
