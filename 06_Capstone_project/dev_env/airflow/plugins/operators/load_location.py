from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadLocationOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 open_aq_conn=None,
                 country=None,
                 *args, **kwargs):

        super(LoadLocationOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.country = country

    def execute(self, context):
        
        db = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        # TODO: use open_aq hook and load data
        self.log.info('Loading dimtable --> {}'.format(self.table))
        db.run(query)
        self.log.info('Load finished!')

