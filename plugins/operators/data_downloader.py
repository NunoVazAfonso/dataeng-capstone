import pandas as pd
import requests

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator


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


        
class CovidDataDownloader(BaseOperator):

	@apply_defaults
	def __init__(self
		,destination_folder = "" 
		,*args, **kwargs):

		super(CovidDataDownloader, self).__init__(*args, **kwargs)

		self.destination_folder = destination_folder

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
		destination_path = self.destination_folder 

		cases_df.to_csv( destination_path + "/covid_data.csv", sep = ',' )
		daily_vaccinations.to_csv( destination_path + "/vaccination_data.csv", sep = ',' )

		self.log.info('Covid Data Downloader: Complete')
