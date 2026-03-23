from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries


S3_BUCKET = Variable.get('s3_bucket', default_var='jammal-s3')
S3_LOG_DATA = Variable.get('s3_log_data', default_var='log-data')
S3_SONG_DATA = Variable.get('s3_song_data', default_var='song-data')
S3_LOG_JSON = Variable.get('s3_log_json', default_var='log_json_path.json')
S3_LOG_PREFIX = Variable.get('s3_log_prefix', default_var=f'{S3_LOG_DATA}/2018/11')


default_args = {
    'owner': 'jammal',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'start_date': datetime(2018, 11, 1)
}

dag = DAG('final_project',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False,
          max_active_runs=1
        )

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

create_tables = SQLExecuteQueryOperator(
    task_id='Create_tables',
    conn_id='redshift',
    sql=SqlQueries.create_tables,
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_events',
    s3_bucket=S3_BUCKET,
    s3_key=S3_LOG_PREFIX,
    json_path=f's3://{S3_BUCKET}/{S3_LOG_JSON}',
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    table='staging_songs',
    s3_bucket=S3_BUCKET,
    s3_key=S3_SONG_DATA,
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

start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [
    load_artist_dimension_table,
    load_song_dimension_table,
    load_time_dimension_table,
    load_user_dimension_table
]
[load_artist_dimension_table, 
 load_song_dimension_table, 
 load_time_dimension_table, 
 load_user_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator