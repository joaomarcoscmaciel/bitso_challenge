import requests
import yaml

# Load Metabase credentials from config file
with open('../etl/config/config.yml') as file:
    config = yaml.safe_load(file)

MB_URL = config['metabase']['url']
MB_USER = config['metabase']['username']
MB_PASS = config['metabase']['password']

# Log in to Metabase and get session token
def metabase_login():
    login_url = f"{MB_URL}/api/session"
    payload = {
        "username": MB_USER,
        "password": MB_PASS
    }
    response = requests.post(login_url, json=payload)
    if response.status_code == 200:
        return response.json().get('id')
    else:
        raise Exception("Failed to log in to Metabase")

# Create metric in Metabase
def create_metric(query_name, query_sql, database_id):
    headers = {
        'Content-Type': 'application/json',
        'X-Metabase-Session': metabase_login()
    }
    create_card_url = f"{MB_URL}/api/card"
    payload = {
        "name": query_name[0],
        "dataset_query": {
            "type": "native",
            "native": {
                "query": query_sql
            },
            "database": database_id
        },
        "display": query_name[1],
        "visualization_settings": {},  # Add this to avoid the error
        "description": f"Metric for {query_name[0]}"  # Optional: add a description
    }
    response = requests.post(create_card_url, json=payload, headers=headers)
    if response.status_code == 200:
        print(f"Metric '{query_name[0]}' created successfully.")
    else:
        print(f"Failed to create metric '{query_name[0]}': {response.text}")

# Read queries from the SQL file and create metrics
def create_metrics_from_file():
    with open('queries.sql') as file:
        queries = file.read().split(';')

    query_names = [
        ["Active Users Per Day", "bar"],
        ["Users Without Deposit", "table"],
        ["Users With More Than 5 Deposits", "table"],
        ["Last Login Per User", "table"],
        ["Logins Between Dates", "bar"],
        ["Unique Currencies Deposited Per Day", "bar"],
        ["Unique Currencies Withdrew Per Day", "bar"],
        ["Total Amount Deposited Per Currency Per Day", "bar"]
    ]

    for i, query_sql in enumerate(queries):
        if query_sql.strip():
            create_metric(query_names[i], query_sql.strip(), database_id=2)  # Adjust DB ID as needed

if __name__ == "__main__":
    create_metrics_from_file()
