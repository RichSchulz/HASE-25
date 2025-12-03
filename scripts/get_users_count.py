"""
Get the total number of users in each country analyzed from our
custom BigQuery table.
"""

from google.cloud import bigquery
from dotenv import load_dotenv
import sys


def get_users_count(client: bigquery.Client, user_table_id: str) -> int:
    query = f"""
    SELECT COUNT(DISTINCT login) AS unique_login_count
    FROM `{user_table_id}`
    """

    query_job = client.query(query)
    df = query_job.result().to_dataframe()
    return df.loc[0, "unique_login_count"]


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
    load_dotenv(override=True)

    if not confirm_action("💰💰💰 This will use Google Cloud credits. Continue?"):
        print("👋 bye")
        return
    
    client = bigquery.Client()
    
    try:
        countries = ["austria", "france", "italy"]

        for country in countries:
            user_table_id = f"hase-25-project.users.{country}"         
            users_count = get_users_count(client, user_table_id)
            print(f"Users count {country}: {users_count}")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
