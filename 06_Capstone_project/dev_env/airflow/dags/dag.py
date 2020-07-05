from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from operators import LoadLocationOperator, LoadDataOperator, CreateTableOperator, DataQualityOperator, LoadDimensionOperator
from helpers import SqlQueries

api_key = Variable.get('open_weather_app_id')

default_args = {
        'owner': 'gonmeso',
        'start_date': datetime(2020, 7, 4),
        'depends_on_past': False,
        # 'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'email_on_retry': False
}

dag = DAG('AirQuality_And_Weather',
          default_args=default_args,
          description='Get data from Weather and AirQuality and load it to postgresql',
          schedule_interval='@daily',
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_time_table = CreateTableOperator(
    task_id='Create_time_table',
    dag=dag,
    conn_id='postgres',
    table_name='time_dim_table',
)

create_weather_table = CreateTableOperator(
    task_id='Create_weather_table',
    dag=dag,
    conn_id='postgres',
    table_name='weather_dim_table',
)

create_location_table = CreateTableOperator(
    task_id='Create_location_table',
    dag=dag,
    conn_id='postgres',
    table_name='location_dim_table',
)

create_staging_table = CreateTableOperator(
    task_id='Create_staging_table',
    dag=dag,
    conn_id='postgres',
    table_name='staging_table',
)

create_measures_table = CreateTableOperator(
    task_id='Create_measures_table',
    dag=dag,
    conn_id='postgres',
    table_name='measures_fact_table',
)
 
populate_location_table = LoadLocationOperator(
    task_id='populate_location_table',
    dag=dag,
    open_aq_conn='open_aq',
    postgres_conn_id='postgres',
    country='ES'
)

load_weather_and_air_quality_data = LoadDataOperator(
    task_id='Load_weather_and_air_quality_data',
    dag=dag,
    postgres_conn_id='postgres',
    open_aq_conn='open_aq',
    open_weather_conn='open_weather',
    app_id=api_key,
    limit=1,
    date=datetime(2020, 7, 4, 0, 0),
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    postgres_conn_id='postgres',
    table='time_dim_table',
    sql=SqlQueries.insert_time,
)

load_weather_dimension_table = LoadDimensionOperator(
    task_id='Load_weather_dim_table',
    dag=dag,
    postgres_conn_id='postgres',
    table='weather_dim_table',
    sql=SqlQueries.insert_weather,
    values=['main', 'description']
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id='postgres',
    table=None, # If table is none, means that quality checks are done to all tables
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Level 1
start_operator >> create_location_table
start_operator >> create_time_table
start_operator >> create_weather_table
start_operator >> create_staging_table

#Level 2
create_location_table >> create_measures_table
create_location_table >> populate_location_table
create_time_table >> create_measures_table
create_weather_table >> create_measures_table
create_staging_table >> create_measures_table

#Level 3
create_measures_table >> load_weather_and_air_quality_data
populate_location_table >> load_weather_and_air_quality_data

# Level 4
load_weather_and_air_quality_data >> load_time_dimension_table
load_weather_and_air_quality_data >> load_weather_dimension_table

# Level 5
load_time_dimension_table >> run_quality_checks
load_weather_dimension_table >> run_quality_checks

# Level 6
run_quality_checks >> end_operator
