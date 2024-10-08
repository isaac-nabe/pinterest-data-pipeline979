from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta


# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': "<your_file_path>",  # make sure to remove any sensitive info
}


# Define params for Run Now Operator
notebook_params = {
    "Variable": 5
}


default_args = {
    'owner': 'Isaac Harrison',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0affe460a4c9_dag',
         # should be a datetime format
         start_date=(2024, 8, 21),
         # check out possible intervals, should be a string
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='<your_cluster_ID>',  # Removed sensitive information
        notebook_task=notebook_task
    )
    opr_submit_run
