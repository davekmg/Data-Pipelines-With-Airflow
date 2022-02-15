from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator

from operators.stage_to_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from sql_queries import SqlQueries

default_args = {
    'owner': 'David',
    'start_date': datetime(2018, 11, 1),
    #'start_date': datetime.now(), # for quick testing
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': True,
    'depends_on_past': True,
}

dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly",
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_access_key_id="aws_access_key",
    aws_secret_key_id="aws_secret_key",
    s3_bucket="udacity-dend",    
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    #s3_key="log_data", # for quick testing
    s3_json_path="s3://udacity-dend/log_json_path.json",
    s3_region="us-west-2"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_access_key_id="aws_access_key",
    aws_secret_key_id="aws_secret_key",    
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_json_path="auto",
    s3_region="us-west-2"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    select_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    select_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    select_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    select_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    select_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    tables=[
            {'name': 'users', 'column_name':'userid'}, 
            {'name': 'songs', 'column_name':'songid'}, 
            {'name': 'artists', 'column_name':'artistid'},
            {'name': 'songplays', 'column_name':'songid'},
            {'name': 'songplays', 'column_name':'artistid'},
            {'name': 'time', 'column_name':'hour'},
        ],
    redshift_conn_id="redshift",
)

end_operator = DummyOperator(task_id='End_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
