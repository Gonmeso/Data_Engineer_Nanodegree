from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 s3_prefix=None,
                 iam_role_arn=None,
                 table=None,
                 json_path='auto',                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.s3_prefix = s3_prefix
        self.iam_role_arn = iam_role_arn 
        self.table = table
        self.json_path = json_path

    def execute(self, context):
        
        query = SqlQueries.staging_table_copy.format(
            self.table,
            self.s3_prefix,
            self.iam_role_arn,
            self.json_path            
        )
        
        PostgresHook(postgres_conn_id=self.redshift_conn_id).run(query)






