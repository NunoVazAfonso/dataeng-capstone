import pandas as pd
import requests

from airflow.models import BaseOperator

class CovidDataDownloader(BaseOperator):


	def execute(self, context):
		self.log.info('Covid Data Downloader: Started')

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
		destination_folder = "{{ var.value.input_folder }}/data" 

		cases_df.to_csv( destination_folder + "/covid_data.csv", sep = ',' )
		daily_vaccinations.to_csv( destination_folder + "/vaccination_data.csv", sep = ',' )


		self.log.info('Covid Data Downloader: Complete')
