import pandas as pd
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import S3Handler

import pycountry
import boto3
import time

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
			S3Handler.upload_file( 
				self.aws_credentials_id,
				self.s3_bucket ,
				destination_path + "/" + file , 
				"capstone_raw/" + file
			)

		self.log.info('Upload to s3: Complete')