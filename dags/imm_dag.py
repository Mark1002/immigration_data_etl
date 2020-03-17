"""dag for testing EMR."""
import logging
import airflowlib.emr_lib as emr

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 12, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG(
    'us_immigration_etl', concurrency=3,
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
    args = kwargs.get('pyspark_file_args', '')
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = emr.get_cluster_dns(cluster_id)
    headers = emr.create_spark_session(cluster_dns, 'pyspark')
    session_url = emr.wait_for_idle_session(cluster_dns, headers)
    statement_response = emr.submit_statement(
        session_url, kwargs['file_path'], args
    )
    emr.track_statement_progress(cluster_dns, statement_response.headers)
    emr.kill_spark_session(session_url)


def transform_i94mapping_to_parquet(**kwargs):
    """Converts all i94mapping text data to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/i94mapping.py'
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
    execution_date = kwargs['execution_date']
    month_year = execution_date.strftime('%b').lower() + execution_date.strftime('%y') # noqa
    logging.info(month_year)
    kwargs['pyspark_file_args'] = "month_year = '{0}'\n".format(month_year)
    submit_emr(**kwargs)


def transform_imm_city_demographics_to_parquet(**kwargs):
    """Converts imm_city_demographics to parquet."""
    kwargs['file_path'] = '/root/airflow/dags/transform/imm_city_demographics.py' # noqa
    submit_emr(**kwargs)


create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transform_i94mapping = PythonOperator(
    task_id='transform_i94mapping',
    python_callable=transform_i94mapping_to_parquet,
    dag=dag)

transform_airport_detail = PythonOperator(
    task_id='transform_airport_detail',
    python_callable=transform_airport_detail_to_parquet,
    dag=dag)

transform_cities_demographics = PythonOperator(
    task_id='transform_cities_demographics',
    python_callable=transform_cities_demographics_to_parquet,
    dag=dag)

transform_immigration = PythonOperator(
    task_id='transform_immigration',
    python_callable=transform_immigration_to_parquet,
    dag=dag)

transform_imm_city_demographics = PythonOperator(
    task_id='transform_imm_city_demographics',
    python_callable=transform_imm_city_demographics_to_parquet,
    dag=dag)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

create_cluster >> wait_for_cluster_completion >> transform_i94mapping
transform_i94mapping >> transform_airport_detail >> transform_immigration
transform_i94mapping >> transform_cities_demographics >> transform_immigration
transform_immigration >> transform_imm_city_demographics
transform_imm_city_demographics >> terminate_cluster
