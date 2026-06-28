# from airflow.sdk import DAG
# from airflow.providers.standard.operator.bash import BashOperator
# from airflow.providers.standard.operator.python import PythonOperator
# dag = DAG(dag_id = "myFirstDag")
# def printContext(**kwargs):
#     print(**kwargs)
#     print("job done")
# copy_file = BashOperator(dag = dag , tash_id = 'copy_file' , bash_commond = "echo copying file")
# tash2 = PythonOperator(dag = dag , task_id = 'printContext' , pyhton_callable = 'printContext')


# copy_file >> task2

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id="myFirstDag",
    start_date = datetime(2026, 6, 27),  # ← start_date
    schedule = '*/2 * * * *',
    catchup=False,
)


def printContext(**kwargs):
    print(kwargs)          
    print("job done")

copy_file = BashOperator(
    dag=dag,
    task_id='copy_file',
    bash_command="echo copying file",
)

task2 = PythonOperator(
    dag=dag,
    task_id='printContext',
    python_callable=printContext,
)

copy_file >> task2