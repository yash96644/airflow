from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import  BranchPythonOperator
from datetime import datetime
# import requests
# import json

def decideFlow(**context):
    checkout_amount = 50
    if checkout_amount < 100:
        return 'checkout'
    else:
        return 'merged'
    
dag = DAG(
    dag_id="pipline_layout_dag",
    start_date=datetime(2026, 6, 28),
    schedule='0 0 * * *',
)

start = EmptyOperator(task_id = "start" , dag = dag)
end = EmptyOperator(task_id = "end" , dag = dag , trigger_rule = "none_failed")
product_page = BashOperator(task_id = "product_page" , bash_command = "echo product page" , dag = dag)
checkout_page = BashOperator(task_id = "checkout_page" , bash_command = "echo checkout page" , dag = dag)
payment_page = BashOperator(task_id = "payment_page" , bash_command = "echo payment page" , dag = dag)

raw_data = BranchPythonOperator(task_id = 'raw_data' , python_callable = decideFlow , dag = dag)


checkout =BashOperator(task_id = "checkout" , bash_command = "echo checkout this page" , dag = dag)
merged = BashOperator(task_id = "merged" , bash_command = "echo merged checkout this page" , dag = dag)
alerming =BashOperator(task_id = 'alerming' , bash_command = "echo alerming this page" , dag = dag)
notify = BashOperator(task_id = 'notify' , bash_command = "echo notify this page" , dag = dag)


start >> [product_page , checkout_page , payment_page] 
[product_page , checkout_page , payment_page]>> raw_data 
raw_data >>[checkout , merged]
checkout >> alerming 
merged >> notify
[alerming , notify] >> end