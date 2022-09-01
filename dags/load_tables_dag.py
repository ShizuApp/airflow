"""
Load/copy data in JSON format from a AWS S3 bucket,
transform the data into a star schema in Redshift, and
check data integrity with custom queries
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)

#from plugins.helpers.sql_queries import SqlQueries
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'shizu',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG('load_data',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="public.staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    dag=dag,
    conn_id="redshift",
    aws_credentials="aws_credentials",
    table="public.staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_path="auto"
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    dag=dag,
    conn_id='redshift',
    table='public.songplays',
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='load_user_dim_table',
    dag=dag,
    conn_id='redshift',
    table='public.users',
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='load_song_dim_table',
    dag=dag,
    conn_id='redshift',
    table='public.songs',
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='load_artist_dim_table',
    dag=dag,
    conn_id='redshift',
    table='public.artists',
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='load_time_dim_table',
    dag=dag,
    conn_id='redshift',
    table='public."time"',
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='data_quality_check',
    dag=dag,
    conn_id='redshift',
    cases = [
        {'sql' : 'SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL', 'answer': 0},
        {'sql' : 'SELECT COUNT(*) FROM public.users WHERE first_name IS NULL', 'answer': 0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Dependencies configuration
start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift]

[stage_events_to_redshift,
stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
    load_song_dimension_table]

[load_user_dimension_table,
load_artist_dimension_table,
load_time_dimension_table,
load_song_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
