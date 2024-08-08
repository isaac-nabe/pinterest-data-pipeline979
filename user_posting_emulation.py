import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from typing import Tuple
import yaml


random.seed(100)

class AWSDBConnector:

    def read_db_creds(self, file_name: str) -> Tuple[dict, str]:
        """
        Reads the database credentials from a YAML file and returns them as a dictionary
        along with the credentials type ('AWS' or 'LOCAL').

        :param file_name: The name of the YAML file containing the database credentials.
        :return: A tuple containing a dictionary of the database credentials and a string indicating the type.
        """
        # Load credentials from YAML file
        with open(file_name, 'r') as file:
            creds = yaml.safe_load(file)
        print(f"Credentials loaded from {file_name}")  # Debugging output

        # Determine the type of credentials based on the keys in the dictionary
        if 'AWS_USER' in creds:
            creds_type = 'AWS'
        elif 'LOCAL_USER' in creds:
            creds_type = 'LOCAL'
        else:
            raise ValueError("Invalid credentials file. Must contain either AWS_USER or LOCAL_USER.")
        
        return creds, creds_type
        
    def create_db_connector(self):
        creds, creds_type = self.read_db_creds('db_creds.yaml')
        
        if creds_type == 'AWS':
            engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{creds['AWS_USER']}:{creds['AWS_PASSWORD']}@{creds['AWS_HOST']}:{creds['AWS_PORT']}/{creds['AWS_DATABASE']}"
            )
        elif creds_type == 'LOCAL':
            engine = sqlalchemy.create_engine(
                f"mysql+pymysql://{creds['LOCAL_USER']}:{creds['LOCAL_PASSWORD']}@{creds['LOCAL_HOST']}:{creds['LOCAL_PORT']}/{creds['LOCAL_DATABASE']}"
            )
        else:
            raise ValueError("Unsupported database type.")
        
        return engine

# Instantiate the connector
new_connector = AWSDBConnector()

def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print("Pinterest Data:", pin_result)
            print("Geolocation Data:", geo_result)
            print("User Data:", user_result)

if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
