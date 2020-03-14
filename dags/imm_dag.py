"""dag for testing EMR."""
import airflowlib.emr_lib as emr

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG(
    'transform_immigration', concurrency=3,
    schedule_interval=None, default_args=default_args
)
region = emr.get_region()
emr.client(region_name=region)


def create_emr(**kwargs):
    """Creates an EMR cluster."""
    cluster_id = emr.create_cluster(
        region_name=region, cluster_name='spark_cluster', num_core_nodes=2
    )
    return cluster_id


def wait_for_completion(**kwargs):
    """Waits for the EMR cluster to be ready to accept jobs."""
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.wait_for_cluster_creation(cluster_id)


def terminate_emr(**kwargs):
    """Terminates the EMR cluster."""
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    emr.terminate_cluster(cluster_id)


def transform_airport_to_parquet(**kwargs):
    """Converts airport mapping to parquet."""
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(
        session_url, '/root/airflow/dags/transform/airport.py'
    )
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transform_airport = PythonOperator(
    task_id='transform_airport',
    python_callable=transform_airport_to_parquet,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

create_cluster >> wait_for_cluster_completion >> transform_airport
transform_airport >> terminate_cluster
