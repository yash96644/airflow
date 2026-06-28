from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import requests
import json

dag = DAG(
    dag_id="data_incremental_dag",
    start_date=datetime(2026, 6, 28),
    schedule='0 0 * * *',
    catchup=True,
)

def fetch_api(url, output_file, **kwargs):
    payload = json.dumps({
        "start_date": "2026-06-28",
        "end_date": "2026-06-28"
    })
    headers = {
        'accept': 'application/json',
        'Authorization': 'Basic YWRtaW46eWFzaA==',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    data = response.json()

    with open(output_file, 'w') as f:
        json.dump(data, f)

    print(f"Data saved to {output_file}")

copy_file = BashOperator(
    dag=dag,
    task_id='copy_file',
    bash_command="echo copying file",
)

task2 = PythonOperator(
    dag=dag,
    task_id='pull_api_data',
    python_callable=fetch_api,
    op_kwargs={
        "url": "http://host.docker.internal:5000/getAll",
        "output_file": '/opt/airflow/output_files/out_file_{{ ds_nodash }}_{{ ts_nodash }}.json'
        #                                                  ^^^^^^^^^^^     ^^^^^^^^^^^^
        #                                                  2026-06-28      har run ka exact time
    }
)

copy_file >> task2