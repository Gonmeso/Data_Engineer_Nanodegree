from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    tables = [
        'artists',
        'songs',
        'time',
        'songplays',
        'users'
    ]
    @apply_defaults
    def __init__(self,
                 redshift_conn_id=None,
                 sql_test=None,
                 table=None,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_test = sql_test
        self.table = table

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.sql_test is None:
            self.sql_test = 'SELECT COUNT(*) FROM {}'
            self.log.info('Query: {}'.format(self.sql_test))
        if self.table is None:
            for t in self.tables:
                self.log.info('Data Quality @  {}'.format(t))
                records = redshift.run(self.sql_test.format(t))
                if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                    self.log.info('Data Quality not passed raising error')
                    raise ValueError('Data Quality not passed for table: {}'.format(t))
        else:
            records = redshift.run(self.sql_test.format(self.table))
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                    self.log.info('Data Quality not passed raising error')
                    raise ValueError('Data Quality not passed for table: {}'.format(t))
        self.log.info('Data Quality passed!')

