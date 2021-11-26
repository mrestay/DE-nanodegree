class EMRHelper:
    @staticmethod
    def job_override(**kwargs):
        job_flow_overrides = {
            "Name": "Movie review classifier",
            "ReleaseLabel": "emr-5.29.0",
            "Applications": [{"Name": "Hadoop"}, {"Name": "Spark"}],  # We want our EMR cluster to have HDFS and Spark
            "Configurations": [
                {
                    "Classification": "spark-env",
                    "Configurations": [
                        {
                            "Classification": "export",
                            "Properties": {"PYSPARK_PYTHON": "/usr/bin/python3"},
                            # by default EMR uses py2, change it to py3
                        }
                    ],
                }
            ],
            "Instances": {
                "InstanceGroups": [
                    {
                        "Name": "Master node",
                        "Market": "SPOT",
                        "InstanceRole": "MASTER",
                        "InstanceType": kwargs["master_instance_type"],
                        "InstanceCount": int(kwargs["master_instance_count"]),
                    },
                    {
                        "Name": "Core - 2",
                        "Market": "SPOT",  # Spot instances are a "use as available" instances
                        "InstanceRole": "CORE",
                        "InstanceType": kwargs["core_instance_type"],
                        "InstanceCount": int(kwargs["core_instance_count"]),
                    },
                ],
                "KeepJobFlowAliveWhenNoSteps": True,
                "TerminationProtected": False,  # this lets us programmatically terminate the cluster
            },
            "JobFlowRole": "EMR_EC2_DefaultRole",
            "ServiceRole": "EMR_DefaultRole",
        }

        return job_flow_overrides

    @staticmethod
    def spark_steps():
        spark_steps = [  # Note the params values are supplied to the operator
            {
                "Name": "Move raw data from S3 to HDFS",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "s3-dist-cp",
                        "--src=s3://{{ params.BUCKET_NAME }}/raw/",
                        "--dest=/home/hadoop/",
                    ],
                },
            },
            {
                "Name": "Copy code to EMR",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "aws",
                        "s3",
                        "cp",
                        f"s3://{{ params.BUCKET_NAME }}/src/",
                        "/home/hadoop/",
                        "--recursive",
                    ],
                },
            },
            {
                "Name": "Run ETL",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "--deploy-mode",
                        "client",
                        '/home/hadoop/src/etl.py'
                    ],
                },
            },
            {
                "Name": "Move clean data from HDFS to S3",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "s3-dist-cp",
                        "--src=/data/trusted/",
                        "--dest=s3://{{ params.BUCKET_NAME }}/{{ params.s3_trusted }}",
                    ],
                },
            },
        ]

        return spark_steps
