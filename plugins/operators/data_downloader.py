import pandas as pd
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import S3Handler

import pycountry
import boto3
import time

class RawDataHandler(BaseOperator):
	"""
	Class downloader of raw data sources from API 
	and low volume sources to be handled locally

	Params: 
		destination_folder (str) - the local path to output raw files to  
		s3_bucket (str) - the S3 destination bucket name
		aws_credentials_id (str) - the airflow credentials variable name for s3 
	"""

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


	def execute(self, context):
		self.log.info('Raw Data Downloader: Started')

		"""
		print('prev date'+context['prev_ds'])
		print(context['execution_date'])
		print('next date'+context['next_ds'])
		"""

		# set origin URLs
		vaccination_data_url = 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/vaccinations/vaccinations.json'
		covid_data_url = 'https://covid19.who.int/WHO-COVID-19-global-data.csv'
		airports_url = 'https://ourairports.com/airports.csv'

		# get URL data 
		daily_vaccinations = pd.json_normalize(
			requests.get(vaccination_data_url).json() # data from request 
			, meta=['country', 'iso_code']
			, record_path= 'data' )

		cases_df = pd.read_csv(covid_data_url)

		airports_df = pd.read_csv(airports_url)
		airports_df = airports_df[["id", "ident", "type", "name", "iso_country", "municipality" ]]
		airports_df.columns = ["id", "code", "type", "name", "iso_country", "municipality" ]

		# save raw data files locally
		destination_path = self.destination_folder 

		cases_df.to_csv( destination_path + "/covid_data.csv", sep = ',' , index = False)
		daily_vaccinations.to_csv( destination_path + "/vaccination_data.csv", sep = ',' , index = False)
		airports_df.to_csv( destination_path + "/airports_data.csv", sep = ',' , index = False)

		# enrich country info
		country_objects = pycountry.countries

		countries = []    
		[ countries.append( {'name': country.name , 'alpha_2': country.alpha_2, 'alpha_3': country.alpha_3 } ) 
		     for country in country_objects ]

		countries_df = pd.DataFrame( countries )

		countries_df.to_csv( destination_path + "/countries_data.csv", sep = ',', index = False)


		self.log.info('Raw Data Downloader: Complete')

		self.log.info('Upload to s3: Started')		

		# upload files to s3 bucket
		files_to_upload = ['covid_data.csv', 'vaccination_data.csv' ,'countries_data.csv', 'airports_data.csv']

		for file in files_to_upload : 
			S3Handler.upload_file( 
				self.aws_credentials_id,
				self.s3_bucket ,
				destination_path + "/" + file , # local path
				"capstone_raw/" + file # s3 destination
			)

		self.log.info('Upload to s3: Complete')