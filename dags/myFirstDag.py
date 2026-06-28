from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def printContext(**kwargs):
    print(kwargs)
    print("job done")

with DAG(
    dag_id="myFirstDag",
    start_date=datetime(2026, 6, 27),
    schedule="*/2 * * * *",
    catchup=False,
) as dag:

    copy_file = BashOperator(
        task_id="copy_file",
        bash_command="echo copying file",
    )

    task2 = PythonOperator(
        task_id="printContext",
        python_callable=printContext,
    )

    copy_file >> task2
