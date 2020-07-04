class SqlQueries:
    create_time_table = """
    CREATE TABLE IF NOT EXISTS time_dim_table
        (measure_date timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int)
        DISTSTYLE ALL;
    """

    create_weather_table = """
    CREATE TABLE IF NOT EXISTS weather_dim_table (
        weather_id SERIAL,
        main varchar(256) NOT NULL,
        description varchar(256) NOT NULL,
        CONSTRAINT weather_pk PRIMARY KEY weather_id
    );
    """

    create_location_table = """
    CREATE TABLE IF NOT EXISTS location_dim_table (
        location_id int NOT NULL,
        country varchar(256),
        city varchar(256),
        lat numeric,
        lon numeric,
        CONSTRAINT location_ok PRIMARY KEY (location_id)
    );
    """

    create_measures_table = """
    CREATE TABLE measures_fact_table (
        measure_id SERIAL,
        measure_date timestamp,
        location_id int,
        weather_id int,
        no2 numeric,
        pm25 numeric,
        pm10 numeric,
        s02 numeric,
        o3 numeric,
        co numeric,
        bc numeric,
        temperature numeric,
        presure numeric,
        humidity numeric,
        clouds numeric,
        wind_speed numeric,
        wind_deg numeric,
        CONSTRAINT measure_pk PRIMARY KEY measure_id
        FOREIGN KEY (location_id) REFERENCES location_dim_table (location_id)
        FOREIGN KEY (weather_id) REFERENCES weather_dim_table (weather_id)
    );
    """
