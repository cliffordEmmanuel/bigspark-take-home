from airflow import DAG
from textwrap import dedent

import pendulum
import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.sql import BranchSQLOperator

from airflow import DAG, XComArg

from datetime import datetime, timedelta


from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


db_conn = "local_postgres"
app_home="/home/clifford/side/projects/bigspark/spark-app"
db_name="batch"


# instantiating a dag object using the context manager
with DAG(
    "batch_etl_with_spark",
    default_args={"retries": 2},
    description="Implements basic batch dag",
    schedule=None,
    start_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
    catchup=False,
    tags=["spark","postgres","batch"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
   

    start = EmptyOperator(
        task_id="start",
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule='none_failed_or_skipped'
    )


    # do batch load here!!!
    batch_load= SparkSubmitOperator(task_id='batch_load',
        conn_id='spark',
        application=f'{app_home}/stretch_task5.py',
        # execution_timeout=timedelta(minutes=10),
        dag=dag
    ) 
    batch_load.doc_md = dedent(
        """\
    #### Extract task
    Testing out the ddls
    """
    )


    start >> batch_load >> end
    