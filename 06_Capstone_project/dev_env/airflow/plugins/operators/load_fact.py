from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadFactOperator(BaseOperator):

    """
    Operator regarding the load of fact tables to the postgres database
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 table=None,
                 values=None,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.values = values

    def execute(self, context):
        """
        Execute the insert in the postgres DB
        """

        if self.values and isinstance(self.values, list):
            self.values = f"({', '.join(self.values)}) "
        else:
            self.values = ""

        query = 'INSERT INTO {} {} {}'.format(
            self.table,
            self.values,
            SqlQueries.insert_measures
        )
        
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        self.log.info('Loading factable --> {}'.format(self.table))
        postgres.run(query)
        self.log.info('Load finished!')
