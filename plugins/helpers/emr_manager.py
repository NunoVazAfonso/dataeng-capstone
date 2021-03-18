
class EmrHandler : 

	instance_type = "m4.xlarge"
	worker_nodes = 2
	key_name = "emr_udacity"

	JOB_FLOW_OVERRIDES = {
	    "Name": "flights_covid_job",
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

	SPARK_STEPS = [ # Note the params values are supplied to the operator
        {
            'Name': 'Mount s3fs and download files',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
                'Args': [
                    's3://udacity-awss/mount_s3fs.sh',
                ]
            }
        },
        {
            'Name': 'Run Spark for downloaded data',
            'ActionOnFailure': 'CANCEL_AND_WAIT',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                     '--deploy-mode',
                     'cluster',
                     '--master',
                     'yarn',
                     'etl.py'
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

		mkdir s3mount

		s3fs udacity-awss /home/hadoop/s3fs-fuse/s3mount -o passwd_file=/home/hadoop/s3fs-fuse/s3fspw1	

		cd /home/hadoop/s3fs-fuse/s3mount

		mkdir flights_raw/

		wget -i capstone_raw/flights_meta -P flights_raw/

		#mkdir tweets_raw/
		#wget -i tweets -P flights_raw/
	"""