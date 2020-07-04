from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class CreateTableOperator(BaseOperator):

    """
    This operator handles the creation of the necessary tables in
    the application database.
    """

    tables = [
        'time_dim_table',
        'weather_dim_table',
        'location_dim_table',
        'measures_fact_table'
    ]

    queries = {
        'time_dim_table': SqlQueries.create_time_table,
        'weather_dim_table': SqlQueries.create_weather_table,
        'location_dim_table': SqlQueries.create_location_table,
        'measures_fact_table': SqlQueries.create_measures_table,
    }

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 table_name=None,
                 *args,
                 **kwargs):

        """
        Operator to create tables given a specific table if the table is not within
        the allowed ones an error will be raised
        """

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table_name = table_name

    def execute(self, context):

        """
        Logic to create the table
        """

        if self.table_name not in self.tables:
            table_msg = 'The indicated tables is not within this list:  {}'.format(
                ' | '.join(self.tables)
            )
            raise ValueError(mestable_msgsage)

        query = self.queries[self.table_name]
        PostgresHook(self.conn_id).run(query)