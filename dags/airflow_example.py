import datetime as dt
import os
import configparser

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from operators.data_downloader import RawDataHandler
from operators.redshift_operator import S3ToRedshiftOperator
from operators.repo_meta import MetadataGetter

from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator

from helpers import SqlQueries, EmrHandler


# set configs
config = configparser.ConfigParser()
config.read( os.path.dirname(os.path.realpath(__file__)) + '/../configs/global.cfg' )

project_root = config.get('PATH', 'PROJECT_ROOT')
input_path = config.get('PATH', 'INPUT_DATA_FOLDER')
output_path = config.get('PATH', 'OUTPUT_DATA_FOLDER')
raw_flight_data_path = input_path + config.get('PATH', 'FLIGHTS_RAW_FOLDER')
raw_tweets_data_path = input_path + config.get('PATH', 'TWEETS_RAW_FOLDER')

flights_repo=config.get('ZENODO', 'FLIGHTS_REPO')
tweets_repo=config.get('ZENODO', 'TWEETS_REPO')

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

""" DONE - getting data to capstone_raw/ in s3 """
"""
get_metadata_task = MetadataGetter(
    task_id="get_metadata",
    dag=dag,
    destination_folder=output_path,
    s3_bucket='udacity-awss',
    aws_credentials_id="s3_credentials",
    project_root=project_root,
    repos=[ 
        {'name': 'flights_meta', 'zenodo_id': flights_repo }, 
        #{'name': 'tweets_meta', 'zenodo_id': tweets_repo }, # TODO: out of scope of this version 
    ]
)
"""
"""
covid_data_task = RawDataHandler(
        task_id = "covid_data_downloader",
        dag = dag,
        destination_folder=output_path,
        s3_bucket='udacity-awss',
        aws_credentials_id="s3_credentials"
    )
"""

""" # DONE - table creation
create_tables_task = PostgresOperator(
        task_id="create_tables",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.create_sttmts 
    )
"""

"""
create_emr_task = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides=EmrHandler.JOB_FLOW_OVERRIDES,
    aws_conn_id="aws_credentials",
    emr_conn_id="emr_connection",
    dag=dag
)
"""

add_emr_steps_taks = EmrAddStepsOperator(
    task_id='add_emr_steps',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=EmrHandler.SPARK_STEPS,
)

""" DONE - Dim and Fact population
populate_staging_task = S3ToRedshiftOperator(
       task_id="populate_staging_tables",
       dag=dag,
       redshift_conn_id="redshift",
       aws_credentials_id="aws_credentials",
       tables= [ 
           {"name": "vaccination_staging", "s3_key" :"capstone_raw/vaccination_data.csv", } ,
           {"name": "covid_staging", "s3_key" :"capstone_raw/covid_data.csv"} ,
           {"name": "countries_staging", "s3_key" :"capstone_raw/countries_data.csv"} ,
           {"name": "tweets_staging", "s3_key" :"tweets.parquet"} ,
           {"name": "flights_staging", "s3_key" :"flights.parquet"} ,
           {"name": "airports_staging", "s3_key" :"airports.parquet"} ,
       ],
       s3_bucket="udacity-awss"
   )

populate_dimensions_task = PostgresOperator(
        task_id="populate_dimensions",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.populate_dims_sttmts 
    )

populate_facts_task = PostgresOperator(
        task_id="populate_facts",
        dag=dag,
        postgres_conn_id="redshift",
        sql=SqlQueries.populate_facts_sttmts 
    )
"""

""" TODO - appropriate dependencies
create_tables_task >> populate_staging_task 
populate_staging_task >> populate_dimensions_task
populate_dimensions_task >> populate_facts_task

"""