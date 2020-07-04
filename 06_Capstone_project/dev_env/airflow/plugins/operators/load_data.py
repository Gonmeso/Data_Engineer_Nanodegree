from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDataOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 open_aq_conn=None,
                 open_weather_con=None,
                 from_date=None,
                 date_to=None,
                 limit=60,
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.open_aq_conn = open_aq_conn
        self.open_weather_con = open_weather_con
        self.from_date = from_date
        self.date_to = date_to
        self.limit = limit

    def execute(self, context):

        # TODO: get data using open_aq and open_weather hooks and insert them
        
        # Get queries form SqlQueries
        query = 'INSERT INTO {} {}'.format(
            self.table,
            SqlQueries.songplay_table_insert
        )
        
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info('Loading factable --> {}'.format(self.table))
        postgres.run(query)
        self.log.info('Load finished!')

