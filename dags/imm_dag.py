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


def submit_emr(**kwargs):
    """Submit spark job to MRR."""
    # ti is the Task Instance
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(
        session_url, kwargs['file_path']
    )
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


def transform_airport_to_parquet(**kwargs):
    """Converts airport mapping to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/airport.py'
    submit_emr(**kwargs)


def transform_state_to_parquet(**kwargs):
    """Converts state mapping to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/state.py'
    submit_emr(**kwargs)


def transform_visa_type_to_parquet(**kwargs):
    """Converts visa_type mapping to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/visa_type.py'
    submit_emr(**kwargs)


def transform_country_to_parquet(**kwargs):
    """Converts country mapping to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/country.py'
    submit_emr(**kwargs)


def transform_transport_type_to_parquet(**kwargs):
    """Converts transport_type mapping to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/transport_type.py'
    submit_emr(**kwargs)


def transform_airport_detail_to_parquet(**kwargs):
    """Converts airport_detail to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/airport_detail.py'
    submit_emr(**kwargs)


def transform_cities_demographics_to_parquet(**kwargs):
    """Converts cities_demographics to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/cities_demographics.py'
    submit_emr(**kwargs)


def transform_immigration_to_parquet(**kwargs):
    """Converts immigration to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/immigration.py'
    submit_emr(**kwargs)


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

transform_state = PythonOperator(
    task_id='transform_state',
    python_callable=transform_state_to_parquet,
    dag=dag)

transform_country = PythonOperator(
    task_id='transform_country',
    python_callable=transform_country_to_parquet,
    dag=dag)

transform_visa_type = PythonOperator(
    task_id='transform_visa_type',
    python_callable=transform_visa_type_to_parquet,
    dag=dag)

transform_transport_type = PythonOperator(
    task_id='transform_transport_type',
    python_callable=transform_transport_type_to_parquet,
    dag=dag)

transform_airport_detail = PythonOperator(
    task_id='transform_airport_detail',
    python_callable=transform_airport_detail_to_parquet,
    dag=dag)

transform_cities_demographics = PythonOperator(
    task_id='transform_cities_demographics',
    python_callable=transform_cities_demographics_to_parquet,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

create_cluster >> wait_for_cluster_completion >> transform_airport
transform_airport >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_state
transform_state >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_country
transform_country >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_visa_type
transform_visa_type >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_transport_type # noqa
transform_transport_type >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_airport_detail # noqa
transform_airport_detail >> terminate_cluster

create_cluster >> wait_for_cluster_completion >> transform_cities_demographics # noqa
transform_cities_demographics >> terminate_cluster
