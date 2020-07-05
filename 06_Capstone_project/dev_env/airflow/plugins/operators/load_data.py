import calendar
from datetime import datetime, timedelta
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
                 date=None,
                 limit=60,
                 *args, **kwargs):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.open_aq_conn = open_aq_conn
        self.open_weather_con = open_weather_con
        self.date = date
        self.limit = limit


    def _weather_date_to_datetime(self, weather_data):
        for idx, hour in enumerate(weather_data):
            hour["dt"] = datetime.utcfromtimestamp(hour["dt"])
            weather_data[idx] = hour
        return weather_data

    def _air_date_to_datetime(self, air_data):
        for idx, measure in enumerate(air_data):
            date = measure["date"]["utc"]
            measure["date"]["utc"] = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            air_data[idx] = measure
        return air_data

    def _get_measures_parameters(self, measures):
        measures_dict = {}
        for measure in measures:
            measures_dict[measure['parameter']] = measure['value']
        return measures_dict


    def _join_air_and_weather_data(self, air_data, weather_data):
        
        joined_data = {}
        for hour in weather_data:
            common_data = [m for m in air_data if m["date"]["utc"] == hour["dt"]]
            param_dict = self._get_measures_parameters(common_data)
            values = (
                hour["temp"],
                hour["pressure"],
                hour["humidity"],
                hour["clouds"],
                hour["wind_speed"],
                hour["wind_deg"],
                hour["temp"],
                hour["temp"],
                hour["temp"],
                hour["temp"],
                param_dict["no2"],
                param_dict["pm25"],
                param_dict["pm10"],
                param_dict["so2"],
                param_dict["o3"],
                param_dict["co"],
                param_dict["bc"],
            )



    def _get_air_data(self, location):

        """
        Gets the air quality data from the API
        """

        open_aq = HttpHook(method='GET', http_conn_id=self.open_aq_conn)
        data = {
            'location': location,
            'date_from': self.date.isoformat(),
            'date_to': (self.date + timedelta(days=1)).isoformat(),
        }
        response = open_aq.run('/v1/measurements', data=data)

        if response.status_code == 200:
            return response.json()['results']
        else:
            raise ValueError

    def _get_weather_data(self, lat, lon):

        """
        Gets the weather data from the specified coordinates and time
        """

        open_weather = HttpHook(method='GET', http_conn_id=self.open_weather_con)
        data = {
            'lat': lat,
            'lon': lon,
            'dt': calendar.timegm(self.date.timetuple()),
            'appid':,
            'units': 'metric'
        }

        response = open_weather.run('/data/2.5/onecall/timemachine', data=data)

        if response.status_code == 200:
            return response.json()['hourly']
        else:
            raise ValueError

    def execute(self, context):

        # TODO: get data using open_aq and open_weather hooks and insert them
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        get_query = "{}  LIMIT {}".format(SqlQueries.get_locations, self.limit) if self.limit is not None else SqlQueries.get_locations
        locations = postgres.get_records(get_query)


        # Get queries form SqlQueries
        query = 'INSERT INTO {} {}'.format(
            self.table,
            SqlQueries.songplay_table_insert
        )
        
        
        self.log.info('Loading factable --> {}'.format(self.table))
        postgres.run(query)
        self.log.info('Load finished!')

