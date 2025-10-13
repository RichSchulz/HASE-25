import os
from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv(override=True)

client = bigquery.Client()

# Perform a query.
QUERY = (
  'SELECT event as issue_status, COUNT(*) as cnt FROM ( '
    'SELECT type, repo.name, actor.login, '
      'JSON_EXTRACT(payload, "$.action") as event, '
    'FROM `githubarchive.day.20190101` '
    'WHERE type = "IssuesEvent" '
  ') '
  'GROUP by issue_status;'
)
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
  print(row)
