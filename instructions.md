1. Install requirements   
  
2. Create variables in airflow
	aws_credentials: login and password as KEY and SECRET. Insert extras {"region_name": "<YOUR_REGION>"}
	emr_connection: similar to aws_credentials (no extras required)
	s3_credentials: similar to emr connection, AWS KEY and SECRET
	redshift: configure connection to redshift

3. Edit configurations file under "configs/global.cfg" 

4. Trigger Airflow run 

