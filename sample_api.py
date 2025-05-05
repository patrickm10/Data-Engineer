import logging
import os
import pandas as pd
import requests
import snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("script.log")
    ]
)
def pull_data_from_api(api_url, params=None):
    """
    Pull data from the given API URL with optional parameters.
    
    :param api_url: The URL of the API endpoint.
    :param params: Optional dictionary of query parameters to include in the request.
    :return: The JSON response from the API.
    """
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()  # Raise an error for bad responses
        logging.info(f"Successfully pulled data from {api_url}")
        logging.debug(f"Response: {response.text}")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error pulling data from API: {e}")
        return None

def save_data_to_csv(data, file_path):
    """
    Save the given data to a CSV file.
    
    :param data: The data to save (should be a list of dictionaries).
    :param file_path: The path where the CSV file will be saved.
    """
    try:
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")
    except Exception as e:
        logging.error(f"Error saving data to CSV: {e}")
        return None

def upload_to_snowflake(df, table_name):
    try:
        sf_user = os.getenv("SNOWFLAKE_USER")
        sf_password = os.getenv("SNOWFLAKE_PASSWORD")
        sf_account = os.getenv("SNOWFLAKE_ACCOUNT")
        sf_database = os.getenv("SNOWFLAKE_DATABASE")
        sf_schema = os.getenv("SNOWFLAKE_SCHEMA")
        sf_warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")

        conn = snowflake.connector.connect(
            user=sf_user,
            password=sf_password,
            account=sf_account,
            warehouse=sf_warehouse,
            database=sf_database,
            schema=sf_schema
        )

        cursor = conn.cursor()
        # Insert logic here
        logging.info(f"Data uploaded to Snowflake table {table_name}")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Error uploading to Snowflake: {e}")


def main():
    api_url = "https://api.example.com/data"
    params = {
        "start_date": "2025-01-01",
        "end_date": "2025-05-05"
    }

    data = pull_data_from_api(api_url, params)

    if data:
        file_path = os.path.join(os.getcwd(), 'api_data.csv')
        df = save_data_to_csv(data, file_path)

        if df is not None:
            table_name = "API_DATA_TABLE"
            upload_to_snowflake(df, table_name)

            # Optional: remove local file after upload
            # os.remove(file_path)
            # logging.info(f"Local file {file_path} removed.")
    else:
        logging.error("No data to process. Exiting.")
        return

    # Pull data from the API
    data = pull_data_from_api(api_url, params)

    if data:
        file_path = os.path.join(os.getcwd(), 'api_data.csv')
        
        save_data_to_csv(data, file_path)

        bucket_name = os.getenv('GCS_BUCKET_NAME')
        if not bucket_name:
            logging.error("GCS_BUCKET_NAME environment variable is not set.")
            return

        destination_blob_name = 'api_data.csv'
        upload_to_snowflake(bucket_name, file_path, destination_blob_name)

        # RM local file if wanted
        # if os.path.exists(file_path):
        #     os.remove(file_path)
        #     logging.info(f"Local file {file_path} removed after upload.")
        # else:
        #     logging.warning(f"Local file {file_path} does not exist.")

    else:
        logging.error("No data to save. Exiting.")
        return
