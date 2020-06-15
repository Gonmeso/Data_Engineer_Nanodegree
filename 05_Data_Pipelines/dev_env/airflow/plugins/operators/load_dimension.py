from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 table=None,
                 truncate=True,
                 sql=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.sql = sql

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            self.log.info('Truncationg table --> {}'.format(self.table))
            redshift.run('TRUNCATE TABLE {}'.format(self.table))

        query = """
                INSERT INTO {} {}
                """.format(
                self.table,
                self.sql
            )
        self.log.info('Loading dimtable --> {}'.format(self.table))
        redshift.run(query)
        self.log.info('Load finished!')

