from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import S3Handler

import time
import zenodo_get

class MetadataGetter(BaseOperator):

	@apply_defaults
	def __init__(self
		,destination_folder = "" 
		,s3_bucket = ""
		,aws_credentials_id = ""
		,repos=[]
		,*args, **kwargs):

		super(MetadataGetter, self).__init__(*args, **kwargs)

		self.destination_folder = destination_folder
		self.s3_bucket = s3_bucket
		self.aws_credentials_id = aws_credentials_id
		self.repos=repos

	def execute(self, context):

		self.log.info('Metadata to S3: Started')

		for repo in self.repos : 
			# get meta files  
			zenodo_get.zenodo_get([
			    "-w", repo['name'], 
			     "-o", self.destination_folder,
			    repo['zenodo_id']
			])

			try: 
				filename = repo['name']

				S3Handler.upload_file( 
					self.aws_credentials_id,
					self.s3_bucket ,
					self.destination_folder + "/" + repo['name'] , 
					"capstone_raw/" + filename
				)
				self.log.info('Uploaded file to S3: {}'.format(filename))
			except : 
				self.log.error('Unable to upload metadata file to S3: repo {}'.format(repo['name']))

		self.log.info('Metadata to S3: Complete')

