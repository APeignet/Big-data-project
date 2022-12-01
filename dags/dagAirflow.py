from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from lib.ingestion import recupDataApiJSON
from lib.formatting import formattingDataJSON
from lib.formatting import formattingDataCSV
from lib.formatting import combineData
from lib.indexing import indexingData

with DAG(
        'dagAirflow',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(seconds=15),
        },
        description='A first DAG',
        schedule_interval=None,
        start_date=datetime(2021, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    dag.doc_md = """
       This is my first DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """

    task1 = PythonOperator(
        task_id='extract_source2',
        python_callable=recupDataApiJSON,
        provide_context=True,
        op_kwargs={'task_number': 'task1'}
    )

    task2 = PythonOperator(
        task_id='format_source2',
        python_callable=formattingDataJSON,
        provide_context=True,
        op_kwargs={'task_number': 'task2'}
    )

    task3 = PythonOperator(
        task_id='format_source1',
        python_callable=formattingDataCSV,
        provide_context=True,
        op_kwargs={'task_number': 'task3'}
    )

    task4 = PythonOperator(
        task_id='combine',
        python_callable=combineData,
        provide_context=True,
        op_kwargs={'task_number': 'task4'}
    )

    task5 = PythonOperator(
        task_id='index',
        python_callable=indexingData,
        provide_context=True,
        op_kwargs={'task_number': 'task5'}
    )

task1 >> task2 >> task4 >> task5
task3 >> task4 >> task5
