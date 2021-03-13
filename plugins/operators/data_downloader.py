import pandas as pd
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator

from airflow.hooks.S3_hook import S3Hook

import pycountry
import boto3

class ZenodoDownloaderOperator(BashOperator):

	@apply_defaults
	def __init__(self, *args, **kwargs):

		kwargs['bash_command']="""
			echo 'Starting URLs download'
			zenodo_get -o {{ var.value.input_folder }}/data -w flights_list 4485741
	        zenodo_get -o {{ var.value.input_folder }}/data -w twitter_list 4568860
	        echo 'Finished URLs download' 

	        cd {{ var.value.input_folder }}/data/

	        # Demo purposes select only a few lines
	        head -2 flights_list > flights
	        head -10 twitter_list > tweets

	        echo "Starting file downloads"

	        echo 'Starting flight data download'
	        wget -i flights -P flights_data/

	        echo 'Starting tweet data download'
	        wget -i tweets -P tweets_data/

	        echo 'Finished file downloads' 
        """

		super(ZenodoDownloaderOperator, self).__init__(*args, **kwargs)

        
class RawDataHandler(BaseOperator):

	@apply_defaults
	def __init__(self
		,destination_folder = "" 
		,s3_bucket = ""
		,aws_credentials_id = ""
		,*args, **kwargs):

		super(RawDataHandler, self).__init__(*args, **kwargs)

		self.destination_folder = destination_folder
		self.s3_bucket = s3_bucket
		self.aws_credentials_id = aws_credentials_id

	def upload_to_s3(
		aws_credentials ,
		s3_bucket="",
		input_file_path="",
		output_file="",
		s3_region="us-west-2" ):

		print("Upload to s3 est")

		s3_hook = S3Hook(aws_conn_id=aws_credentials)

		s3_hook.load_file( filename=input_file_path , bucket_name=s3_bucket , key=output_file)



	def execute(self, context):
		self.log.info('Raw Data Downloader: Started')

		# set origin URLs
		vaccination_data_url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.json'
		covid_data_url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'

		# get URL data 
		daily_vaccinations = pd.json_normalize(
			requests.get(vaccination_data_url).json() # data from request 
			, meta=['country', 'iso_code']
			, record_path= 'data' )

		cases_df = pd.read_csv(covid_data_url)

		# save raw data files
		destination_path = self.destination_folder 

		cases_df.to_csv( destination_path + "/covid_data.csv", sep = ',' , index = False)
		daily_vaccinations.to_csv( destination_path + "/vaccination_data.csv", sep = ',' , index = False)

		# enrich country info
		country_objects = pycountry.countries

		countries = []    
		[ countries.append( {'name': country.name , 'alpha_2': country.alpha_2, 'alpha_3': country.alpha_3 } ) 
		     for country in country_objects ]

		countries_df = pd.DataFrame( countries )

		countries_df.to_csv( destination_path + "/countries_data.csv", sep = ','  , index = False)

		self.log.info('Raw Data Downloader: Complete')


		self.log.info('Upload to s3: Started')		


		files_to_upload = ['covid_data.csv', 'vaccination_data.csv' ,'countries_data.csv']

		for file in files_to_upload : 
			RawDataHandler.upload_to_s3( 
				self.aws_credentials_id,
				self.s3_bucket ,
				destination_path + "/" + file , 
				"capstone_raw/" + file 
			)

		self.log.info('Upload to s3: Complete')