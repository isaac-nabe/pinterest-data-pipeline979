import random
from time import sleep
from sqlalchemy import text
import requests
import yaml
import sqlalchemy
from typing import Tuple
import json

# Import the topic name constants and other configuration details
from config import PINTEREST_TOPIC, GEOLOCATION_TOPIC, USER_TOPIC, API_INVOKE_URL

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


def send_to_api(topic: str, data: dict):
    """
    Sends the data to the API, which will forward it to the MSK Cluster.

    :param topic: The Kafka topic name to which the data should be sent.
    :param data: The data to be sent.
    """
    payload = {
        "topic": topic,
        "data": data
    }

    # Manually serialize the payload to JSON format
    json_payload = json.dumps(payload, default=str)

    # Set the necessary headers
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    # Send the JSON payload to the API
    response = requests.post(
        API_INVOKE_URL, data=json_payload, headers=headers)

    if response.status_code == 200:
        print(f"Successfully sent data to topic {topic}.")
    else:
        print(f"Failed to send data to topic {
              topic}. Response: {response.text}")


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
               