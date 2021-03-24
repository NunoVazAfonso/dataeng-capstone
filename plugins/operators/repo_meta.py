from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import S3Handler, EmrHandler
from airflow.hooks.S3_hook import S3Hook

import zenodo_get

class MetadataGetter(BaseOperator):
	"""
	Processes and generates files with metainformation necessary to process Extract process.
	Fetches Zenodo metadata, necessary to download from Zenodo repository via wget. 
	Generates shell script to execute in ETL machine. 
	Loads Spark ETL files and necessary configurations to S3, to be used in ETL.
	Stores files in S3.

	Params: 
		destination_folder (str) - local path to store generated metadata and process files before uploading to S3
		s3_bucket (str) - the name of s3 bucket to store ETL files
		aws_credentials_id (str) - the name of the AWS connection variable
		project_root (str) - the local root of the project
		repos (Array<dict>) - the Zenodo repo IDs to process 
	"""

	@apply_defaults
	def __init__(self
		,destination_folder = "" 
		,s3_bucket = ""
		,aws_credentials_id = ""
		,project_root = ""
		,repos=[]
		,*args, **kwargs):

		super(MetadataGetter, self).__init__(*args, **kwargs)

		self.destination_folder = destination_folder
		self.s3_bucket = s3_bucket
		self.aws_credentials_id = aws_credentials_id
		self.repos=repos
		self.project_root=project_root

	def execute(self, context):

		self.log.info('Metadata to S3: Started')

		"""
		print('prev date: '+context['prev_ds'])
		print(context['execution_date'])
		print('next date: '+context['next_ds'])
		"""

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

		file_path = self.destination_folder + "/mount_s3fs.sh"

		s3_hook= S3Hook( self.aws_credentials_id ) 

		credentials = s3_hook.get_credentials()

		# write shell script with AWS credentials
		with open(file_path, 'w') as write_file:
			write_file.write(
				EmrHandler.shell_script.format( 
					aws_id = credentials.access_key , 
					aws_key = credentials.secret_key 
				)
			)

		# upload shell script to S3
		try: 
			S3Handler.upload_file( 
				self.aws_credentials_id,
				self.s3_bucket ,
				file_path, 			# local file path
				"mount_s3fs.sh" 	# name tof file to be stored in bucket
			)
			self.log.info('Uploaded file to S3: {}'.format("mount_s3fs.sh"))
		except : 
			self.log.error('Unable to upload metadata file to S3: repo {}'.format("mount_s3fs.sh"))


		# Spark ETL files to S3 (py with main and configs)

		etl_files = [
			{ 'local_path' : self.project_root + '/scripts/' , 'file': 'spark_etl.py' },
			{ 'local_path' : self.project_root + '/configs/' , 'file': 'global.cfg' },
		]

		for f in etl_files :
			try: 
				S3Handler.upload_file( 
					self.aws_credentials_id,
					self.s3_bucket ,
					f['local_path'] + f['file'], 			# local file path
					"etl/" + f['file'] 	# name tof file to be stored in bucket
				)
				self.log.info('Uploaded file to S3: {}'.format(f['file']))
			except : 
				self.log.error('Unable to upload metadata file to S3: repo {}'.format(f['file']))

		# END 
		self.log.info('Metadata to S3: Complete')

