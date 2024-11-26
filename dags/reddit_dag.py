from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os 
import sys
sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipline import reddit_pipline
from pipelines.aws_s3_pipline import upload_s3_pipline



default_args = {
    'owner':'Sushant Shinde',
    'start_date':datetime(2024,11,24)
}

file_postfix = datetime.now().strftime('%Y%m%d')

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['reddit','etl','pipeline']

)

extract = PythonOperator(
    task_id='reddit_extraction',
    python_callable=reddit_pipline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit': 'dataengineering',
        'time_filter': 'day',
        'limit': 100
    },
    dag=dag
)

upload_s3 = PythonOperator(
    task_id='s3_upload',
    python_callable=upload_s3_pipline,
    dag=dag
)

extract >> upload_s3

