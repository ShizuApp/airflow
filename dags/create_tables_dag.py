"""
Run queries to DROP existing tables and CREATE new ones
on the Redshift cluster connected to Airflow connection
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators import CreateTableOperator

#from plugins.helpers.sql_queries import SqlQueries
from helpers import SqlQueries

default_args = {
    'owner': 'shizu',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('create_tables',
          default_args=default_args,
          description='Create tables in Redshift'
        )

create_songplays_table = CreateTableOperator(
    task_id='create_songplays_table',
    dag=dag,
    conn_id='redshift',
    table='public.songplays',
    sql=SqlQueries.songplay_table_create
)

create_artist_table = CreateTableOperator(
    task_id='create_artist_table',
    dag=dag,
    conn_id='redshift',
    table='public.artists',
    sql=SqlQueries.artist_table_create
)

create_user_table = CreateTableOperator(
    task_id='create_user_table',
    dag=dag,
    conn_id='redshift',
    table='public.users',
    sql=SqlQueries.user_table_create
)

create_time_table = CreateTableOperator(
    task_id='create_time_table',
    dag=dag,
    table='public."time"',
    conn_id='redshift',
    sql=SqlQueries.time_table_create
)

create_song_table = CreateTableOperator(
    task_id='create_song_table',
    dag=dag,
    conn_id='redshift',
    table='public.songs',
    sql=SqlQueries.song_table_create
)

create_staging_events_table = CreateTableOperator(
    task_id='create_staging_events_table',
    dag=dag,
    conn_id='redshift',
    table='public.staging_events',
    sql=SqlQueries.stg_events_table_create
)

create_staging_songs_table = CreateTableOperator(
    task_id='create_staging_songs_table',
    dag=dag,
    conn_id='redshift',
    table='public.staging_songs',
    sql=SqlQueries.stg_songs_table_create
)