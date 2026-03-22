from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries


default_args = {
    'owner': 'jammal',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1)
}

dag = DAG('final_project_legacy',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket='jammals3',
    s3_key='log_data/{{ execution_date.year }}/{{ execution_date.month }}',
    json_path='s3://jammals3/log_json_path.json',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket='jammals3',
    s3_key='song_data',
    json_path='auto',
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id='redshift',
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id='redshift',
    table='users',
    sql=SqlQueries.user_table_insert,
    truncate_insert=True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id='redshift',
    table='songs',
    sql=SqlQueries.song_table_insert,
    truncate_insert=True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id='redshift',
    table='artists',
    sql=SqlQueries.artist_table_insert,
    truncate_insert=True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id='redshift',
    table='time',
    sql=SqlQueries.time_table_insert,
    truncate_insert=True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id='redshift',
    dq_checks=[
        {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM songs WHERE song_id IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artist_id IS NULL', 'expected_result': 0},
        {'check_sql': 'SELECT COUNT(*) FROM time WHERE start_time IS NULL', 'expected_result': 0}
    ],
    dag=dag
)

end_operator = EmptyOperator(task_id='Stop_execution', dag=dag)

start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_artist_dimension_table,
    load_song_dimension_table,
    load_time_dimension_table,
    load_user_dimension_table
]
[load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table, load_user_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator