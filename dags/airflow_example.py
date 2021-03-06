import datetime as dt
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from operators.data_downloader import ( ZenodoDownloaderOperator )


def print_world():
    print('world')


default_args = {
    'owner': 'me',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

dag = DAG(
    'nhunh_example',
    default_args=default_args,
    schedule_interval='@monthly'
)
zenodo_downlad_task = ZenodoDownloaderOperator(
        task_id='zenodo_download',
        dag = dag
    )

# url_download_task = BashOperator(
#     task_id='url_download',
#     bash_command= """
#         echo 'Starting URLs download'

#         zenodo_get -o {{ var.value.input_folder }}/data/ -w flights_list 4485741
        
#         zenodo_get -o {{ var.value.input_folder }}/data/ -w twitter_list 4568860

#         echo 'Finished URLs download' 
#     """,
#     dag=dag,
# )

# data_files_download_task = BashOperator(
#     task_id='data_files_download',
#     bash_command= """
#         cd {{ var.value.input_folder }}/data/

#         # Demo purposes select only a few lines
#         head -2 flights_list > flights
#         head -10 twitter_list > tweets

#         echo "Starting file downloads"

#         echo 'Starting flight data download'
#         wget -i flights

#         echo 'Starting tweet data download'
#         wget -i tweets

#         echo 'Finished file downloads' 
#     """,
#     dag=dag,
# )



# url_download_task >> data_files_download_task