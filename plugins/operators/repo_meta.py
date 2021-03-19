from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import S3Handler, EmrHandler
from airflow.hooks.S3_hook import S3Hook

import zenodo_get

class MetadataGetter(BaseOperator):

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

