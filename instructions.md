Instructions  
  
  
1. Install dependencies  
	zenodo_get , pip install zenodo-get  
	pyspark
	boto3
  
2. Create variables in airflow
	project_root: the root folder of the project
	input_folder: folder where pre-staging data will be stored

3. Mount file system in EC2 instance. 
	$ s3fs udacity-awss -o use_cache=/tmp -o allow_other -o uid=1001 -o mp_umask=002 -o multireq_max=5 /home/udacity_mount




References :  
https://kulasangar.medium.com/create-an-emr-cluster-and-submit-a-job-using-boto3-c34134ef68a0  	