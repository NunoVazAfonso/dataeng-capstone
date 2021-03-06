from airflow.operators.bash_operator import BashOperator
from airflow.utils.decorators import apply_defaults

#from helpers import SqlQueries

class ZenodoDownloaderOperator(BashOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):

        super(ZenodoDownloaderOperator, self).__init__(*args, **kwargs)

		self.bash_command = """
		        echo 'Starting URLs download'
		        zenodo_get -o {{ var.value.input_folder }}/data/ -w flights_list 4485741
		        zenodo_get -o {{ var.value.input_folder }}/data/ -w twitter_list 4568860
		        echo 'Finished URLs download'

				cd {{ var.value.input_folder }}/data/

		        # Demo purposes select only a few lines
        		head -2 flights_list > flights
		        head -10 twitter_list > tweets

    		    echo "Starting file downloads"

        		echo 'Starting flight data download'
        		wget -i flights

		        echo 'Starting tweet data download'
        		wget -i tweets

		        echo 'Finished file downloads' 
		"""


        
    def execute(self, context):
        
        self.log.info('Zenodo Downloader')
        

