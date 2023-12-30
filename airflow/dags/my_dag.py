from airflow import DAG
from datetime import datetime
from random import randint
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.operators import spark_submit_operator

from scripts_airflow.script import main



with DAG(dag_id="my_dag",start_date=datetime(2023,1,1),schedule_interval='@daily',catchup=False) as dag:
    main_dag = spark_submit_operator(
        task_id = "my_dag",
        python_callable=main,
    )

main_dag