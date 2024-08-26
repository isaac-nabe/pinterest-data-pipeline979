import random
from time import sleep
from sqlalchemy import text
import requests
import yaml
import sqlalchemy
from typing import Tuple
import json

# Import the topic-specific URLs from config
from config import PIN_API_INVOKE_URL, GEO_API_INVOKE_URL, USER_API_INVOKE_URL

random.seed(100)


class AWSDBConnector:

    def read_db_creds(self, file_name: str) -> Tuple[dict, str]:
        """
        Reads the database credentials from a YAML file and returns them as a dictionary
        along with the credentials type ('AWS' or 'LOCAL').
        """
        # Load credentials from YAML file
        with open(file_name, 'r') as file:
            creds = yaml.safe_load(file)
        print(f"Credentials loaded from {file_name}")

        # Determine the type of credentials based on the keys in the dictionary
        if 'AWS_USER' in creds:
            creds_type = 'AWS'
        elif 'LOCAL_USER' in creds:
            creds_type = 'LOCAL'
        else:
            raise ValueError(
                "Invalid credentials file. Must contain either AWS_USER or LOCAL_USER.")

        return creds, creds_type

    def create_db_connector(self):
        creds, creds_type = self.read_db_creds('db_creds.yaml')

        if creds_type == 'AWS':
            engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{creds['AWS_USER']}:{creds['AWS_PASSWORD']}@{
                    creds['AWS_HOST']}:{creds['AWS_PORT']}/{creds['AWS_DATABASE']}"
            )
        elif creds_type == 'LOCAL':
            engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{
                    creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}"
            )
        else:
            raise ValueError("Unsupported database type.")

        return engine


# Instantiate the connector
new_connector = AWSDBConnector()


def send_to_api(url: str, data: dict):
    """
    Sends the data to the API.

    :param url: The full API URL for the specific topic.
    :param data: The data to be sent.
    """
    # Manually serialize the payload to JSON format
    json_payload = json.dumps({
        "records": [
            {
                "value": data
            }
        ]}, default=str)

    # Set the necessary headers
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    # Send the JSON payload to the API
    response = requests.post(url, data=json_payload, headers=headers)

    if response.status_code == 200:
        print(f"Successfully sent data to {url}.")
    else:
        print(response.status_code)
        print(f"Failed to send data to {url}. Response: {response.text}")


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(
                f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)

            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(
                f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)

            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(
                f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)

            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Send data to the corresponding API URLs
            send_to_api(PIN_API_INVOKE_URL, pin_result)
            send_to_api(GEO_API_INVOKE_URL, geo_result)
            send_to_api(USER_API_INVOKE_URL, user_result)

            print("Data sent to API for Kafka ingestion")


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
