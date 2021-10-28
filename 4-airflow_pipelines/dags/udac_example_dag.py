from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

default_args = {
    "owner": "sparkify",
    "depends_on_past": False,
    "start_date": datetime(2021, 10, 21),
    "end_date": datetime(2021, 10, 22),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "catchup": False,
    "email_on_retry": False,
    "s3_json_path": "s3://udacity-dend/log_json_path.json",
    "s3_bucket": "udacity-dend",
    "s3_region": "us-east-1",
}

with DAG(
    "build_dwh",
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@daily",
) as dag:
    start_operator = DummyOperator(task_id="Begin_execution")

    create_staging_events_table = PostgresOperator(
        task_id="Create_staging_events_table",
        sql=SqlQueries.staging_events_table_create,
    )

    create_staging_songs_table = PostgresOperator(
        task_id="Create_staging_songs_table",
        sql=SqlQueries.staging_songs_table_create,
    )

    create_songplays_table = PostgresOperator(task_id="Create_songplays_table", sql=SqlQueries.songplays_table_create)

    create_songs_table = PostgresOperator(task_id="Create_songs_table", sql=SqlQueries.songs_table_create)

    create_users_table = PostgresOperator(task_id="Create_users_table", sql=SqlQueries.users_table_create)

    create_artists_table = PostgresOperator(task_id="Create_artists_table", sql=SqlQueries.artists_table_create)

    create_time_table = PostgresOperator(task_id="Create_time_table", sql=SqlQueries.time_table_create)

    tables_created = DummyOperator(task_id="tables_created")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        table="stage_events",
        s3_bucket=default_args["s3_bucket"],
        s3_key="log_data",
        s3_region=default_args["s3_region"],
        s3_json_path=default_args["s3_json_path"],
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        table="staging_songs",
        s3_bucket=default_args["s3_bucket"],
        s3_key="song_data/A/A/A",
        s3_region=default_args["s3_region"],
        s3_json_path="auto",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        table="songplays",
        columns=[
            "playid",
            "start_time",
            "userid",
            "level",
            "songid",
            "artistid",
            "sessionid",
            "location",
            "user_agent",
        ],
        sql_statement=SqlQueries.songplay_table_insert,
        truncate=True,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        table="users",
        columns=["userid", "first_name", "last_name", "gender", "level"],
        sql_statement=SqlQueries.user_table_insert,
        truncate=True,
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        table="songs",
        columns=["songid", "title", "artistid", "year", "duration"],
        sql_statement=SqlQueries.song_table_insert,
        truncate=True,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        table="artists",
        columns=["artistid", "name", "location", "lattitude", "longitude"],
        sql_statement=SqlQueries.artist_table_insert,
        truncate=True,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        table="time",
        columns=["start_time", "hour", "day", "week", "month", "year", "weekday"],
        sql_statement=SqlQueries.time_table_insert,
        truncate=True,
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks", tables=["songplays", "artists", "users", "time", "songs"]
    )

    end_operator = DummyOperator(task_id="Stop_execution")

create_tables = [
    create_staging_events_table,
    create_staging_songs_table,
    create_songplays_table,
    create_songs_table,
    create_users_table,
    create_artists_table,
    create_time_table,
]

load_dim_tables = [
    load_song_dimension_table,
    load_artist_dimension_table,
    load_user_dimension_table,
    load_time_dimension_table,
]

stage_data = [stage_events_to_redshift, stage_songs_to_redshift]

start_operator >> create_tables >> tables_created
tables_created >> stage_data
stage_data >> load_songplays_table >> load_dim_tables >> run_quality_checks
run_quality_checks >> end_operator
