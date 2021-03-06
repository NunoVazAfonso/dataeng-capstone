from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    """
    Handles load of data to Redshift destination DB 
    based on raw files stored in S3. 

    Attributes : 
        copy_sql (str) - base copy statement to load batch data from file to table
        parquet_format (str) - specifies parquet format to append to copy sttmt if raw input is parquet file 
        csv_format (str) - specifies csv format and meta to append to copy sttmt if raw input is csv file
    
    Params:  
        redshift_conn_id (str) - the Redshift configured connection name in airflow
        aws_credentials_id (str) - the AWS configured connection name in airflow
        tables (Array<dict>) - the destination tables to process the raw data files to
        s3_bucket (str) - the bucket name of the S3 raw data files  
        delimiter (str) - the csv delimiter (default comma)  
        ignore_headers (int) - 1 or 0 - ignore csv header first row (default 1)

    """

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    parquet_format = " FORMAT AS PARQUET "

    csv_format = """
        IGNOREHEADER {}
        DELIMITER '{}'
        CSV QUOTE '"'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 tables=[],
                 s3_bucket="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = S3Hook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables : 

            table_copy_sql = ""

            if "csv" in table['s3_key'] : 
                table_copy_sql = S3ToRedshiftOperator.copy_sql + S3ToRedshiftOperator.csv_format
            elif "parquet" in table['s3_key'] :
                table_copy_sql = S3ToRedshiftOperator.copy_sql + S3ToRedshiftOperator.parquet_format
            else :
                table_copy_sql = S3ToRedshiftOperator.copy_sql

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE {}".format(table['name']))

            self.log.info("Copying data from S3 to Redshift")
            rendered_key = table['s3_key'].format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = table_copy_sql.format(
                table['name'],
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formatted_sql)
