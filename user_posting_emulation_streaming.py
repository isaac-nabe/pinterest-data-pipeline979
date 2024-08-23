import random
from time import sleep
from sqlalchemy import text
import requests
import yaml
import sqlalchemy
from typing import Tuple
import json

# Define the Kinesis stream URLs from the config file
from config import PIN_API_INVOKE_URL, GEO_API_INVOKE_URL, USER_API_INVOKE_URL, PIN_STREAM, GEO_STREAM, USER_STREAM

# Set a seed for the random number generator to ensure reproducibility
random.seed(100)


class AWSDBConnector:
    """
    A class to handle database connections for AWS and local databases.
    """

    def read_db_creds(self, file_name: str) -> Tuple[dict, str]:
        """
        Reads the database credentials from a YAML file and returns them as a dictionary
        along with the credentials type ('AWS' or 'LOCAL').

        :param file_name: The name of the YAML file containing the database credentials.
        :return: A tuple containing the credentials dictionary and the credentials type.
        """
        # Load credentials from the specified YAML file
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
        """
        Creates a SQLAlchemy engine based on the type of database credentials provided.

        :return: A SQLAlchemy engine object for database interaction.
        """
        # Read the credentials from the YAML file
        creds, creds_type = self.read_db_creds('db_creds.yaml')

        # Create the engine based on whether the credentials are for AWS or local
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


# Instantiate the database connector
new_connector = AWSDBConnector()


def send_to_kinesis(url: str, stream_name: str, data: dict):
    """
    Sends data to the Kinesis stream via the API Gateway.

    :param url: The full API URL for the specific topic.
    :param stream_name: The name of the Kinesis stream.
    :param data: The data to be sent as a dictionary.
    """
    # Serialize the payload to JSON format, including the stream name and partition key
    json_payload = json.dumps({
        "StreamName": stream_name,
        "Data": json.dumps(data),
        "PartitionKey": "partition-key"  # Replace with a suitable partition key if needed
    })

    # Set the necessary headers for the request
    headers = {'Content-Type': 'application/json'}

    # Send the JSON payload to the API using the PUT method
    response = requests.request("PUT", url, headers=headers, data=json_payload)

    # Check the response status and print appropriate messages
    if response.status_code == 200:
        print(f"Response Status Code: {response.status_code}. Successfully sent data to {url}.")
    else:
        print(response.status_code)
        print(f"Failed to send data to {url}. Response: {response.text}")


def run_infinite_post_data_loop():
    """
    Runs an infinite loop that continuously posts data from the database to the Kinesis streams.
    """
    while True:
        # Sleep for a random interval between 0 and 2 seconds
        sleep(random.randrange(0, 2))

        # Select a random row from the database
        random_row = random.randint(0, 11000)

        # Create a new database connection
        engine = new_connector.create_db_connector()

        # Retrieve a random record from the Pinterest data table
        with engine.connect() as connection:
            pin_string = text(
                f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            # Retrieve a random record from the Geolocation data table
            geo_string = text(
                f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            # Retrieve a random record from the User data table
            user_string = text(
                f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            for row in user_selected_row:
                user_result = dict(row._mapping)

            # Make sure timestamp formats are converted to a string so they are JSON serializable
            geo_result['timestamp'] = geo_result['timestamp'].isoformat()
            user_result['date_joined'] = user_result['date_joined'].isoformat()

            # Send data to the corresponding Kinesis streams via the API Gateway
            send_to_kinesis(PIN_API_INVOKE_URL, PIN_STREAM, pin_result)
            send_to_kinesis(GEO_API_INVOKE_URL, GEO_STREAM, geo_result)
            send_to_kinesis(USER_API_INVOKE_URL, USER_STREAM, user_result)

            print("Data sent to Kinesis streams via API Gateway")


if __name__ == "__main__":
    # Run the infinite data posting loop
    run_infinite_post_data_loop()
    print('Working')
