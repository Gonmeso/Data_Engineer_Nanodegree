from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

cfg = Variable.get('sparkify', deserialize_json=True)

default_args = {
        'owner': 'udacity',
        'start_date': datetime(2020, 6, 12),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'email_on_retry': False
}

dag = DAG('Project_4_Dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    s3_prefix=cfg['s3']['log_data'],
    iam_role_arn=cfg['iam_role'],
    table=cfg['redshift']['table_staging_events'],
    json_path=cfg['s3']['log_data_jsonpath']
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    redshift_conn_id='redshift',
    s3_prefix=cfg['s3']['events_data'],
    iam_role_arn=cfg['iam_role'],
    table=cfg['redshift']['table_staging_songs']
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=cfg['redshift']['table_songplays']
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=cfg['redshift']['table_users'],
    truncate=True,
    sql=SqlQueries.users_table_insert,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=cfg['redshift']['table_songs'],
    truncate=True,
    sql=SqlQueries.songs_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=cfg['redshift']['table_artists'],
    truncate=True,
    sql=SqlQueries.artists_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table=cfg['redshift']['table_time'],
    truncate=True,
    sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    table=None, # If table is none, means that quality checks are done to all tables
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Level 1
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

#Level 2
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

#Level 3
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

# Level 4
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

# Level 5
run_quality_checks >> end_operator
