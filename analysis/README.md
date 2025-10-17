# ChatGPT Ban Analysis

This folder contains the analysis of Italy's ChatGPT ban impact on GitHub commit activity.

## Overview

This analysis examines whether Italy's temporary ban on ChatGPT (April 1-28, 2023) affected software development activity as measured by GitHub commits. We use a difference-in-differences approach comparing Italy (treatment) with Austria and France (control groups).

## Sample Size

- **Italy**: 1000 developers sampled (treatment group)
- **Austria**: 1000 developers sampled (control group)
- **France**: 1000 developers sampled (control group)
- **Total**: 3000 developers sampled

**Note**: Not all sampled users will have commits in the GitHub Archive during the analysis period. This is normal because:

- GitHub Archive only captures public activity
- Not all users are active during any given time period
- Some users may have private repositories or different activity patterns

The analysis focuses on the **active subset** of developers who had commits during the period, which is actually more meaningful for understanding development activity patterns.

## Data Collection

Before running the analysis, collect the data using the updated script:

```bash
# Collect data for Italy (1000 users)
python ../gh-archive-bigquery/fetch_commit_events.py ../github_scrape/data/github_users_merged_italy.csv --sample 1000

# Collect data for Austria (1000 users)
python ../gh-archive-bigquery/fetch_commit_events.py ../github_scrape/data/github_users_merged_austria.csv --sample 1000

# Collect data for France (1000 users)
python ../gh-archive-bigquery/fetch_commit_events.py ../github_scrape/data/github_users_merged_france.csv --sample 1000
```

## Analysis

Run the Jupyter notebook `chatgpt_ban_analysis.ipynb` to perform the complete analysis including:

1. **Data Loading and Preprocessing**
2. **Daily Commit Trends Visualization**
3. **Statistical Analysis** (Before vs During vs After Ban)
4. **Difference-in-Differences Estimation**
5. **Statistical Significance Testing**
6. **Conclusions and Interpretation**

## Key Dates

- **Analysis Period**: January 26 - May 26, 2023
- **ChatGPT Ban**: April 1-28, 2023 (full period)
- **Ban Duration Options**:
  - **3-day ban**: April 1-3, 2023 (immediate impact)
  - **7-day ban**: April 1-7, 2023 (before adaptation)
  - **Full ban**: April 1-28, 2023 (includes adaptation period)
- **Pre-ban Period**: January 26 - March 31, 2023
- **Post-ban Period**: April 29 - May 26, 2023

## Ban Duration Analysis

The analysis accounts for the fact that people likely adapted to the ban after a few days (using VPNs, etc.). You can experiment with different ban durations:

- **3-day ban**: Captures immediate impact before adaptation
- **7-day ban**: Captures impact before widespread adaptation
- **Full ban**: Includes the adaptation period (may dilute effects)

## Expected Outputs

The analysis will produce:

- Daily commit trend visualizations
- Difference-in-differences plots
- Statistical significance tests
- Effect size calculations
- Comprehensive interpretation of results

## Requirements

- Python 3.8+
- pandas, numpy, matplotlib, seaborn, scipy
- Jupyter notebook
- Data files from `../gh-archive-bigquery/output/`
