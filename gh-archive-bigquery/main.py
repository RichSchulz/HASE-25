from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(override=True)

client = bigquery.Client()

date_str = "20230403"  # Format: YYYYMMDD

# Query github data for 'PushEvent's at a certain date.
# We should porbably filter this for users for which we know
# that they are located in Italy, e.g., with the below condition
# added to the WHERE clause of the temporary `parsed` table below:
# "AND actor.login = '{username}'"
#
# Form the commit hashes we though this, we should then be able
# to get the actual commits containing the changes

QUERY = f"""
WITH parsed AS (
  SELECT
    actor.login AS user,
    repo.name AS repository,
    JSON_EXTRACT_ARRAY(payload, "$.commits") AS commits,
    created_at
  FROM
    `githubarchive.day.{date_str}`
  WHERE
    type = 'PushEvent'
)
SELECT
  user,
  repository,
  created_at AS event_time,
  JSON_VALUE(c, "$.sha") AS commit_sha,
  JSON_VALUE(c, "$.message") AS commit_message
FROM
  parsed,
  UNNEST(commits) AS c
ORDER BY
  event_time DESC
LIMIT 100
"""

query_job = client.query(QUERY)
df = query_job.to_dataframe()

output_path = f"gh_archive_{date_str}.csv"
df.to_csv(output_path, index=False, encoding="utf-8")

print(f"✅ Saved {len(df)} rows to {output_path}")
