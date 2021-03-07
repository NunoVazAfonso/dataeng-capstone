import datetime as dt
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from operators.data_downloader import ZenodoDownloaderOperator, CovidDataDownloader


default_args = {
    'owner': 'nunovazafonso',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

dag = DAG(
    'nhunh_example',
    default_args=default_args,
    schedule_interval='@monthly'
)

zenodo_task = ZenodoDownloaderOperator(
        task_id = "zenodo_downloader",
        dag = dag
)

covid_data_task = CovidDataDownloader(
        task_id = "covid_data_downloader",
        dag = dag,
        destination_folder= Variable.get("input_folder") + "/data/covid"
    )

