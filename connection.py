import os

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

credentials_secret = os.environ.get("gcp_service_account", "")
credentials_path = ".secrets/sp_api_bq_creds.json"
if not os.path.exists(credentials_path) and not credentials_secret:
    raise BaseException("Creds for BQ are mising")


def create_credentials():
    if credentials_secret:
        credentials = Credentials.from_service_account_info(credentials_secret)
    else:
        credentials = Credentials.from_service_account_file(credentials_path)
    return credentials


def connect_to_bigquery():
    credentials = create_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client
