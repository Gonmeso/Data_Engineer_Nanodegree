from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadDimensionOperator(BaseOperator):

    """
    Operator regarding the load of dimension tables to the postgres database
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 table=None,
                 truncate=False,
                 sql=None,
                 values=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table = table
        self.truncate = truncate
        self.sql = sql
        self.values = values

    def execute(self, context):

        """
        Execute the insert in the postgres DB
        """

        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        if self.truncate:
            self.log.info('Truncationg table --> {}'.format(self.table))
            postgres.run('TRUNCATE TABLE {}'.format(self.table))

        # Let Postgres handle de PK (SERIAL field)
        if self.values and isinstance(self.values, list):
            self.values = f"({', '.join(self.values)}) "
        else:
            self.values = ""

        query = """
                INSERT INTO {} {} {}
                """.format(
                self.table,
                self.values,
                self.sql)

        self.log.info('Loading dimtable --> {}'.format(self.table))
        postgres.run(query)
        self.log.info('Load finished!')
