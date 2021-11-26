from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_terminate_job_flow import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from helpers.sql_queries import SqlQueries
from helpers.emr import EMRHelper
from operators.data_quality import DataQualityOperator

default_args = {
    "owner": "mrestay",
    "depends_on_past": False,
    "start_date": datetime(2021, 11, 21),
    "end_date": datetime(2021, 11, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "email_on_retry": False,
    "s3_bucket": "nd-capstone-data",
    "s3_region": "us-east-1",
    "emr_args": {
        "master_instance_type": "m4.xlarge",
        "master_instance_count": 1,
        "core_instance_type": "m4.xlarge",
        "core_instance_count": 2,
    },
}

args = {
    "immigration": {
        "query": SqlQueries.immigration_fact_create,
        "table": "immigration",
        "s3_key": "trusted/immigration_fact/",
    },
    "flights": {"query": SqlQueries.flights_dimension_create, "table": "flights", "s3_key": "trusted/flights_dim/"},
    "visitors": {"query": SqlQueries.visitors_dimension_create, "table": "visitors", "s3_key": "trusted/visitors_dim/"},
    "dates": {"query": SqlQueries.dates_dimension_create, "table": "dates", "s3_key": "trusted/dates_dim/"},
    "visas": {"query": SqlQueries.visas_dimension_create, "table": "visas", "s3_key": "trusted/visas_dim/"},
    "general_demographics": {
        "query": SqlQueries.general_demographics_dimension_create,
        "table": "general_demographics",
        "s3_key": "trusted/general_demog_dim/",
    },
    "race_demographics": {
        "query": SqlQueries.race_demographics_dimension_create,
        "table": "race_demographics",
        "s3_key": "trusted/race_demog_dim/",
    },
    "port_codes": {
        "query": SqlQueries.port_codes_dimension_create,
        "table": "port_codes",
        "s3_key": "trusted/port_codes_dim/",
    },
    "airports": {
        "query": SqlQueries.airports_dimension_create,
        "table": "airports",
        "s3_key": "trusted/airports_dim/",
    },
}


with DAG(
    "build_dwh",
    default_args=default_args,
    description="Extract  Load and transform data with Spark into Redshift with Airflow",
    schedule_interval="@once",
) as dag:
    start_operator = DummyOperator(task_id="Begin_execution")

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=EMRHelper.job_override(
            **{
                "master_instance_type": default_args["emr_args"]["master_instance_type"],
                "master_instance_count": default_args["emr_args"]["master_instance_count"],
                "core_instance_type": default_args["emr_args"]["core_instance_type"],
                "core_instance_count": default_args["emr_args"]["core_instance_count"],
            }
        ),
        # aws_conn_id="aws_default",
        # emr_conn_id="emr_default",
    )

    add_emr_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=EMRHelper.spark_steps(),
        params={  # these params are used to fill the paramterized values in SPARK_STEPS json
            "BUCKET_NAME": default_args["s3_bucket"],
            "s3_clean": "trusted/",
        },
    )

    last_step = len(EMRHelper.spark_steps()) - 1  # this value will let the sensor know the last step to watch
    # wait for the steps to complete
    emr_step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
        step_id=f"{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[{str(last_step)}] }}",
        # aws_conn_id="aws_default",
    )

    terminate_emr_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    )

    create_tables = [
        PostgresOperator(task_id=f"Create_{name}_table", sql=arg_dict["query"]) for name, arg_dict in args.items()
    ]

    tables_created = DummyOperator(task_id="tables_created")

    transfer_s3_to_redshift = [
        S3ToRedshiftOperator(
            s3_bucket=default_args["s3_bucket"],
            s3_key=arg_dict["s3_key"],
            schema="PUBLIC",
            table=arg_dict["table"],
            copy_options=["FORMAT AS PARQUET"],
            task_id=f"transfer_s3_to_redshift_{name}",
        )
        for name, arg_dict in args.items()
    ]

    tables_loaded = DummyOperator(task_id="tables_loaded")

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks", tables=[arg_dict["table"] for arg_dict in args.values()]
    )

    end_operator = DummyOperator(task_id="Stop_execution")

start_operator >> create_emr_cluster >> add_emr_steps >> emr_step_checker >> terminate_emr_cluster
terminate_emr_cluster >> create_tables >> tables_created
tables_created >> transfer_s3_to_redshift >> tables_loaded
tables_loaded >> run_quality_checks >> end_operator
