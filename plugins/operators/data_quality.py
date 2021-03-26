from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Data quality checker for Redshift tables. 
    Check for non-empty tables.
    Dims check for uniqueness according to column reference identifier.

    Params: 
         redshift_conn_id (str) - the aiflow connection variable name
         tables (Array<str>) - the tables to check for count

    Raises: 
        ValueError - if count is 0 or null
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 tables = [], # array of tables to check for rows
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Data Quality check: Started')
        
        try :
            redshift = PostgresHook( self.redshift_conn_id )            
        except :
            self.log.error( 'Error establishing Data Quality Redshift connection' )
        
        # check count
        row_count_stmt = "SELECT count(1) FROM {}"

        
        for table in self.tables:     

            self.log.info('Checking rows in {}'.format(table) )

            table_count = redshift.get_records( row_count_stmt.format(table) )
            
            if len(table_count) < 1 or len(table_count[0]) < 1:
                raise ValueError( 'Validation failed for Table {}: No records found'.format(table))
            num_records = table_count[0][0]
            if num_records < 1:
                raise ValueError( 'Validation failed for Table {}: 0 rows'.format(table))
            else :
                self.log.info( 'Validation passed for table {}: {} records found'.format(table, num_records) )


        # check dims for duplicate codes
        duplicate_row_stmt = "SELECT {id_col}, count(1) FROM {table_name} GROUP BY {id_col} HAVING count(1)> 1"

        dim_tables = [t for t in self.tables if 'dim' in t ]
        id_dictionary = { 
            'dim_country': 'iso2', 
            'dim_airport': 'code',
            'dim_airline': 'code' 
        }

        for dim_t in dim_tables :     
            if dim_t in id_dictionary: 
                
                check_column = id_dictionary[dim_t]

                self.log.info('Checking duplicate {} for {}'.format(check_column, dim_t) )

                code_count = redshift.get_records( 
                    duplicate_row_stmt.format(
                        id_col = check_column, 
                        table_name = dim_t) 
                    )

                if len(code_count) > 0 and len(code_count[0]) > 0:                
                    num_records = code_count[0][1]
                    if num_records > 0 :
                        raise ValueError( 'Table {} has duplicate {}: {} rows. Validation FAILED. '.format(dim_t, check_column, num_records ))
                
                self.log.info( 'Table {} has no duplicate {}: Validation SUCCESS. '.format(dim_t, check_column) )                
                

        self.log.info('Data Quality check: Complete')