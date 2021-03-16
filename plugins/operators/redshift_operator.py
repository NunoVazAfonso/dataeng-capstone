from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):

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
                    y="",
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
            redshift.run("DELETE FROM {}".format(table['name']))

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
