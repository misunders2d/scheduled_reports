from os.path import exists

from google.cloud import bigquery
from google.oauth2.service_account import Credentials

credentials_path = ".secrets/sp_api_bq_creds.json"
if not exists(credentials_path):
    raise BaseException("Creds for BQ are mising")


def create_credentials():
    credentials = Credentials.from_service_account_file(credentials_path)
    return credentials


def connect_to_bigquery():
    credentials = create_credentials()
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    return client
