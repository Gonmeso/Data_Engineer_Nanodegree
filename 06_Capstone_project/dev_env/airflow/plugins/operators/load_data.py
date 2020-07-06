import calendar
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.http_hook import HttpHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.sql_queries import SqlQueries


class LoadDataOperator(BaseOperator):

    """
    Operator that performs the main logic of retrieving the data and transform it
    into a usable one. Retrives the data from both of the sources Open AQ and
    Open Weather using an HttpHook.
    """

    ui_color = "#F98866"

    @apply_defaults
    def __init__(
        self,
        postgres_conn_id=None,
        open_aq_conn=None,
        open_weather_conn=None,
        app_id=None,
        date=None,
        limit=60,
        *args,
        **kwargs,
    ):

        super(LoadDataOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.open_aq_conn = open_aq_conn
        self.open_weather_conn = open_weather_conn
        self.app_id = app_id
        self.date = date
        self.limit = limit

    def _weather_date_to_datetime(self, weather_data):
        """
        Change weather data timestamp to datetime for common ground with AQ data

        :param weather_data: hourly data from request to Open Weather
        :return: same input with updated date format
        """

        for idx, hour in enumerate(weather_data):
            hour["dt"] = datetime.utcfromtimestamp(hour["dt"])
            weather_data[idx] = hour
        return weather_data

    def _air_date_to_datetime(self, air_data):
        """
        Change air data string date to datetime for common ground with weather data

        :param air_data: hourly data from request to Open AQ
        :return: same input with updated date format
        """

        for idx, measure in enumerate(air_data):
            date = measure["date"]["utc"]
            measure["date"]["utc"] = datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
            air_data[idx] = measure
        return air_data

    def _get_measures_parameters(self, measures):
        """
        Create a dictionary to be filled with existing measures, if no measures
        are found, the insert will contain nulls. Transforms a list of dictionaries
        of measures into a single one.

        :param measures: list of measures dictionaries
        :return: single dictionary with measures values
        """

        measures_dict = {
            "no2": "NULL",
            "pm25": "NULL",
            "pm10": "NULL",
            "so2": "NULL",
            "o3": "NULL",
            "co": "NULL",
            "bc": "NULL",
        }

        # Update dict with measures values
        for measure in measures:
            measures_dict[measure["parameter"]] = measure["value"]

        self.log.info(f"Current measures: {measures_dict}")
        return measures_dict

    def _join_air_and_weather_data(self, air_data, weather_data):
        """
        Join Air Quality data and weather data based on the timestamp. Checks
        the dates of the weather data and matches them to zero or more
        parameter measures from Open AQ.

        :param air_data: response from Open AQ API with updated dates
        :param weather_data: response from Open Weather API with updated dates
        :return: a list of tuples containing the rows values
        """

        row_list = []
        for hour in weather_data:
            # Check common dates from hourly weather data to hourly parameters measures
            common_data = [m for m in air_data if m["date"]["utc"] == hour["dt"]]
            if len(common_data) == 0:
                continue
            param_dict = self._get_measures_parameters(common_data)

            # Create the tuple
            values = (
                hour["dt"],
                self.current_location,
                hour["temp"],
                hour["pressure"],
                hour["humidity"],
                hour["clouds"],
                hour["wind_speed"],
                hour["wind_deg"],
                hour["weather"][0]["main"],
                hour["weather"][0]["description"],
                param_dict["no2"],
                param_dict["pm25"],
                param_dict["pm10"],
                param_dict["so2"],
                param_dict["o3"],
                param_dict["co"],
                param_dict["bc"],
            )
            row_list.append(values)

        return row_list

    def _get_air_data(self, location):

        """
        Gets the air quality data from the API.

        :param location: location to be used as query param.
        :return: response retrieved from the API
        """

        self.current_location = location
        self.log.info(f"########## Current Location: {location} ########## ")
        open_aq = HttpHook(method="GET", http_conn_id=self.open_aq_conn)
        data = {
            "location": location,
            "date_from": self.date.isoformat(),
            "date_to": (self.date + timedelta(days=1)).isoformat(),
        }
        response = open_aq.run("/v1/measurements", data=data)

        if response.status_code == 200:
            self.log.info("Air data successfully retrived from location")
            return self._air_date_to_datetime(response.json()["results"])
        else:
            raise ValueError

    def _get_weather_data(self, lat, lon):

        """
        Gets the weather data from the specified coordinates and time

        :param lat: latitude to be used as query param.
        :param lon: longitude to be used as query param.
        :return: response retrieved from the API
        """

        open_weather = HttpHook(method="GET", http_conn_id=self.open_weather_conn)
        data = {
            "lat": lat,
            "lon": lon,
            "dt": calendar.timegm(self.date.timetuple()),
            "appid": self.app_id,
            "units": "metric",
        }

        response = open_weather.run("/data/2.5/onecall/timemachine", data=data)

        if response.status_code == 200:
            self.log.info("Weather data successfully retrived from location")
            return self._weather_date_to_datetime(response.json()["hourly"])
        else:
            raise ValueError

    def execute(self, context):

        # Create hook and retrieve previously loaded locations
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        get_query = (
            "{}  LIMIT {}".format(SqlQueries.get_locations, self.limit)
            if self.limit is not None
            else SqlQueries.get_locations
        )
        locations = postgres.get_records(get_query)

        # Join and insert data for each location retrieved
        for loc_id, lat, lon in locations:
            air = self._get_air_data(loc_id)
            weather = self._get_weather_data(lat, lon)

            rows = self._join_air_and_weather_data(air, weather)

            for row in rows:
                self.log.info(row)
                query = SqlQueries.insert_staging.format(*row)
                postgres.run(query)
        self.log.info("Load finished!")

