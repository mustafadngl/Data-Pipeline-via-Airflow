from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'depends_on_paste': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False, #or equvalent argument 'catchup_on_default'= False
}
#In order to check data quality following default 'check_quary' list can be manuplated in order to sql query and expectation value.
check_query=[
        {"check_sql":"SELECT COUNT(*) FROM songplays WHERE playid IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM artists WHERE artistid IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM songs WHERE songid IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM time WHERE start_time IS NULL","expected_value":0},
        {"check_sql":"SELECT COUNT(*) FROM users WHERE userid IS NULL","expected_value":0},
    ]
#In order to check length and quantity via table names are listed. 
table_names=["artists","songplays","songs","time","users","staging_events","staging_songs"]

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs= 1,
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id ='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data/{execution_date.year}/{execution_date.month}/{execution_date.day}',
    region='us-west-2',
    data_format='s3://udacity-dend/log_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id ='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key ='song_data',
    region='us-west-2',
    data_format='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    truncate=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql= SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql= SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    sql= SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    checking_vars= check_query,
    tables=table_names
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table, load_time_dimension_table] 
[load_user_dimension_table,load_song_dimension_table,load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator