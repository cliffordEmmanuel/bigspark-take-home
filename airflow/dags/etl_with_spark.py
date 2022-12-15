from airflow import DAG
from textwrap import dedent

import pendulum
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.sql import BranchSQLOperator

from airflow import DAG, XComArg

from datetime import datetime, timedelta
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


db_conn = "local_postgres"
app_home="/home/clifford/side/projects/bigspark/spark-app"
db_name="data_5g"


def _create_db(db_name):
    # TODO: make these variables hidden
    conn = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    # setting isolation level so this Create cmd runs outside the transaction.
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    cursor.execute(f"CREATE DATABASE {db_name};")
    

# instantiating a dag object using the context manager
with DAG(
    "etl_with_spark",
    default_args={"retries": 2},
    description="Implements a basic etl with spark",
    schedule=None,
    start_date=pendulum.datetime(2022, 11, 1, tz="UTC"),
    catchup=False,
    tags=["spark","postgres"],
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

    # dummy operators used for the branching...
    present = EmptyOperator(
        task_id="is_present",
    )

    absent = EmptyOperator(
        task_id="is_absent",
    )


    create_db = PythonOperator(
        task_id="create_db",
        python_callable=_create_db,
        op_kwargs={
            "db_name":db_name,
        }
    )

    create_and_populate= SparkSubmitOperator(task_id='create_and_populate',
        conn_id='spark',
        application=f'{app_home}/stretch_task3.py',
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )

    create_and_populate.doc_md = dedent(
        """\
    #### Extract task
    Testing out the ddls
    """
    )

    check_db_exist = BranchSQLOperator(
        task_id="check_if_data_5g_exists",
        conn_id=db_conn,
        sql=f"SELECT coalesce((SELECT 1 FROM pg_database where datname = '{db_name}'),0);",
        follow_task_ids_if_true="is_present",
        follow_task_ids_if_false=["is_absent","create_db","create_and_populate"],
        dag=dag,
        do_xcom_push=False
    )

    start >> check_db_exist >> [present, absent] 
    absent >> create_db>>create_and_populate >> end
    present >> end
