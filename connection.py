from google.cloud import bigquery
from google.oauth2 import service_account


def create_credentials():
    credentials_path = ".secrets/sp_api_bq_creds.json"
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path
    )
    return credentials


def connect_to_bigquery():
    credentials = create_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client
