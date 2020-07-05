from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries

class LoadLocationOperator(BaseOperator):

    """
    Handles the step of loading locations to the database
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id=None,
                 open_aq_conn=None,
                 endpoint='/v1/locations',
                 country=None,
                 *args, **kwargs):

        super(LoadLocationOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.open_aq_conn = open_aq_conn
        self.endpoint = endpoint
        self.country = country

    def _insert_each_location(self, postgres_hook, data):

        """
        Helper function to insert each location recieved from the response
        """
        for location in data:
            loc_id = location.get("location")
            country = location.get("country")
            city = location.get("city")
            lat = location.get("coordinates").get("latitude")
            lon = location.get("coordinates").get("longitude")

            query = SqlQueries.insert_location.format(loc_id, country, city, lat, lon)
            self.log.info('Loading the following location: {}-{}-{}-{}-{}'.format(
                loc_id, country, city, lat, lon
            ))
            postgres_hook.run(query)
            self.log.info('Location inserted successfully')


    def execute(self, context):
        
        db = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        open_aq = HttpHook(method='GET', http_conn_id=self.open_aq_conn)
        data = {
            'country': self.country,
            'limit': 1000,
        }
        response = open_aq.run(self.endpoint, data=data)

        if response.status_code == 200:
            self._insert_each_location(db, response.json()["results"])
        else:
            raise ValueError(f"Status Code is {response.status_code}")

        self.log.info('Load finished!')

