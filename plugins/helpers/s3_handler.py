from airflow.hooks.S3_hook import S3Hook

class S3Handler:

	def upload_file(
		aws_credentials ,
		s3_bucket="",
		input_file_path="",
		output_file="",
		s3_region="us-west-2" ):

		s3_hook = S3Hook( aws_conn_id=aws_credentials )

		# load file
		s3_hook.load_file( 
			filename=input_file_path 
			, bucket_name=s3_bucket 
			, key=output_file
			, replace=True)