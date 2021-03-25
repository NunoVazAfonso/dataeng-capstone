import time

class EmrHandler : 
	"""
	Class to instantiate EmrHandler object.
	Configurations for instantiating an EMR Job flow and
	for adding Steps for Spark ETL process 

	Attributes:
		s3_bucket - (str) - your working s3 bucket name
		instance_type - (str) - type of EMR master and worker nodes
		worker_nodes - (int) - nr of worker nodes
		JOB_FLOW_OVERRIDES - (str) - configurations for EMR cluster
		SPARK_STEP_MOUNT - (str) - steps to add mount on EMR cluster
		SPARK_STEP_SPARK - (str) - steps to add Spark ETL process to EMR cluster 
		shell_script - (str) - shell script to upload to S3 to automate zenodo files download 
	"""

	s3_bucket='udacity-awss'

	instance_type = "m5.xlarge"
	worker_nodes = 3
	key_name = "emr_udacity"


	JOB_FLOW_OVERRIDES = {
	    "Name": "flights_covid_job_" + time.strftime('%Y%m%d%H%M%S',time.gmtime()) ,
	    "ReleaseLabel": "emr-6.2.0",
	    "Applications": [ 
	    	{"Name": "Spark"} 
    	],
	    "Instances": {
	        "InstanceGroups": [
	            {
	                "Name": "Master node",
	                "Market": "SPOT",
	                "InstanceRole": "MASTER",
	                "InstanceType": instance_type,
	                "InstanceCount": 1,
	            },
	            {
	                "Name": "Core",
	                "Market": "SPOT", # Spot instances are a "use as available" instances. Best is ON_DEMAND, but pricier.
	                "InstanceRole": "CORE",
	                "InstanceType": instance_type,
	                "InstanceCount": worker_nodes,
	            },
	        ],
	        "Ec2KeyName" : key_name,
	        "KeepJobFlowAliveWhenNoSteps": True,
	        "TerminationProtected": False, # terminate the cluster programmaticaly
	    },
	    "VisibleToAllUsers": True, 
	    "JobFlowRole": "EMR_EC2_DefaultRole",
	    "ServiceRole": "EMR_DefaultRole",
	}

	##################################

	SPARK_STEP_MOUNT = [ # Note the params values are supplied to the operator
        {
            'Name': 'Mount s3fs and download files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': [
                    's3://'+ s3_bucket +'/mount_s3fs.sh',
                ]
            }
        }
	]
	
	SPARK_STEP_SPARK = [
       {
            'Name': 'Run Spark for downloaded data',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                     '--deploy-mode',
                     'cluster',
                     #'--master',
                     #'yarn',
                     's3a://' + s3_bucket +  '/etl/spark_etl.py'
                ]
            }
        }
    ]


	shell_script = """

		sudo yum update all -y
		sudo yum install automake fuse fuse-devel gcc-c++ git libcurl-devel libxml2-devel make openssl-devel -y

		cd /home/hadoop

		git clone https://github.com/s3fs-fuse/s3fs-fuse.git

		cd s3fs-fuse
		./autogen.sh
		./configure --prefix=/usr --with-openssl

		make
		sudo make install

		echo {aws_id}:{aws_key} >> s3fspw1

		sudo chmod 600 s3fspw1

		mkdir -p s3mount

		s3fs """ + s3_bucket + """ /home/hadoop/s3fs-fuse/s3mount -o passwd_file=/home/hadoop/s3fs-fuse/s3fspw1	

		cd /home/hadoop/s3fs-fuse/s3mount

		mkdir -p flights_raw/

		wget -i capstone_raw/flights_meta -nc -P flights_raw/ 

		#mkdir -p tweets_raw/
		#wget -i tweets -nc -P flights_raw/
	"""
