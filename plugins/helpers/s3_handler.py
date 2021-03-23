from airflow.hooks.S3_hook import S3Hook

class S3Handler:
	"""
	Instantiates common helper object to interact with S3 bucket
	"""
	def upload_file(
		aws_credentials ,
		s3_bucket="",
		input_file_path="",
		output_file="",
		s3_region="us-west-2" ):
		"""
		Upload file to S3 bucket

		Params:
			aws_credentials (str) - the configured airflow credentials for AWS
			s3_bucket (str) - the name of the destination s3 bucket
			input_file_path (str) - path to the local file to upload
			output_file (str) - the key to which the file will upload to in destination bucket
			s3_region (str) - s3 bucket region
		"""
		s3_hook = S3Hook( aws_conn_id=aws_credentials )

		# load file
		s3_hook.load_file( 
			filename=input_file_path 
			, bucket_name=s3_bucket 
			, key=output_file
			, replace=True)