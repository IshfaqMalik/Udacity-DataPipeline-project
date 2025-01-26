from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from udacity.common.final_project_sql_statements import SqlQueries


default_args = {
    'owner': 'ishfaq malik',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay':timedelta(minutes=3),
    
    

}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table = 'staging_events',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = 'udacity-pipeline',
        s3_key = 'log-data/',
        json_path = 's3://udacity-pipeline/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table = 'staging_songs',
        redshift_conn_id = 'redshift',
        aws_credentials_id = 'aws_credentials',
        s3_bucket = 'udacity-pipeline',
        s3_key = 'song-data/',
        json_path ='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id = 'redshift',
        table = 'songplays',
        sql = SqlQueries.songplay_table_insert,
        append_only = False
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id = 'redshift',
        table = 'users',
        sql= SqlQueries.user_table_insert,
        append_only = False

    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id = 'redshift',
        table = 'songs',
        sql = SqlQueries.song_table_insert,
        append_only = False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id = 'redshift',
        table = 'artists',
        sql = SqlQueries.artist_table_insert,
        append_only = False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id = 'redshift',
        table = 'time',
        sql = SqlQueries.time_table_insert,
        append_only= False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id= 'redshift',
        tables=[ "songplays", "songs", "artists",  "time", "users"]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table 
    load_songplays_table >> [load_user_dimension_table,load_song_dimension_table,
                            load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table,load_song_dimension_table,
                            load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks>> end_operator

final_project_dag = final_project()