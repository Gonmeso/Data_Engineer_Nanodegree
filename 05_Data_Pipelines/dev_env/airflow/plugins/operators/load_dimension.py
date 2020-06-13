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
                 pk=None,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.pk = pk

    def execute(self, context):
        
        query_name = '{}_table_insert'.format(self.dim)
        select_query = eval('SqlQueries.{}'.format(query_name)) if query_name in dir(SqlQueries()) else None
        
        if self.truncate:
            query = """
                TRUNCATE TABLE {}
                INSERT INTO {} ({})
            """.format(
                self.table,
                self.table,
                select_query
            )
        elif self.table == 'users':               
             query = """
                 INSERT INTO {} ({})
                 ON CONFLICT ({})
                 DO UPDATE
                 SET level = EXCLUDED.level;
                 """.format(
                 self.table,
                 query_select,
                 self.pk
                )
        else:
            query = """
                 INSERT INTO {} {}
                 ON CONFLICT ({})
                 DO NOTHING;
                 """.format(
                 self.table,
                 query_select,
                 self.pk
                )
        
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(query)

