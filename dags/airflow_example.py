import datetime as dt
import os
import configparser

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable
from operators.data_downloader import ZenodoDownloaderOperator, RawDataHandler



# set configs
config = configparser.ConfigParser()
config.read( os.path.dirname(os.path.realpath(__file__)) + '/../configs/global.cfg' )

input_path = config.get('PATH', 'INPUT_DATA_FOLDER')
output_path = config.get('PATH', 'OUTPUT_DATA_FOLDER')
raw_flight_data_path = input_path + config.get('PATH', 'FLIGHTS_RAW_FOLDER')
raw_tweets_data_path = input_path + config.get('PATH', 'TWEETS_RAW_FOLDER')



default_args = {
    'owner': 'nunovazafonso',
    'start_date': dt.datetime.now(),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

dag = DAG(
    'capstone_project',
    default_args=default_args,
    schedule_interval='@monthly'
)

#zenodo_task = ZenodoDownloaderOperator(
#        task_id = "zenodo_downloader",
#        dag = dag
#)

covid_data_task = RawDataHandler(
        task_id = "covid_data_downloader",
        dag = dag,
        destination_folder= output_path,
        s3_bucket='udacity-awss',
        aws_credentials_id="s3_credentials"
    )

