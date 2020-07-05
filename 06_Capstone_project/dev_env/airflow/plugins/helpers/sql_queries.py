class SqlQueries:
    create_time_table = """
    CREATE TABLE IF NOT EXISTS time_dim_table
        (measure_date timestamp,
        hour int,
        day int,
        week int,
        month int,
        year int);
    """

    create_weather_table = """
    CREATE TABLE IF NOT EXISTS weather_dim_table (
        weather_id SERIAL,
        main varchar(256) NOT NULL,
        description varchar(256) NOT NULL UNIQUE,
        CONSTRAINT weather_pk PRIMARY KEY (weather_id)
    );
    """

    create_location_table = """
    CREATE TABLE IF NOT EXISTS location_dim_table (
        location_id varchar(256),
        country varchar(256),
        city varchar(256),
        lat numeric,
        lon numeric,
        CONSTRAINT location_ok PRIMARY KEY (location_id)
    );
    """

    create_measures_table = """
    CREATE TABLE IF NOT EXISTS measures_fact_table (
        measure_id SERIAL,
        measure_date timestamp,
        location_id varchar(256),
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
        CONSTRAINT measure_pk PRIMARY KEY (measure_id),
        FOREIGN KEY (location_id) REFERENCES location_dim_table (location_id),
        FOREIGN KEY (weather_id) REFERENCES weather_dim_table (weather_id)
    );
    """

    create_staging_table = """
    CREATE TABLE IF NOT EXISTS staging_table (
        location_id varchar(256),
        main varchar(256),
        description varchar(256),
        measure_date timestamp,
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
        wind_deg numeric
    );
    """

    insert_measures = """
    SELECT
        staging.location_id,
        staging.no2,
        staging.pm25,
        staging.pm10,
        staging.s02,
        staging.o3,
        staging.co,
        staging.bc,
        staging.temperature,
        staging.presure,
        staging.humidity,
        staging.clouds,
        staging.wind_speed,
        staging.wind_deg,
        staging.measure_date,
        weather.weather_id
        FROM staging_table staging
        LEFT JOIN weather_dim_table weather
        ON staging.description = weather.description
    """

    insert_staging = """
    INSERT INTO staging_table (
        measure_date,
        location_id,
        temperature,
        presure,
        humidity,
        clouds,
        wind_speed,
        wind_deg,
        main,
        description,
        no2,
        pm25,
        pm10,
        s02,
        o3,
        co,
        bc
    ) VALUES ('{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {})
    """

    insert_location = """
    INSERT INTO location_dim_table (location_id, country, city, lat, lon)
    VALUES ('{}', '{}' ,'{}' , {}, {})
    ON CONFLICT (location_id) DO NOTHING;
    """

    insert_weather = """
        SELECT DISTINCT main, description
        FROM staging_table
        ON CONFLICT (description) DO NOTHING
    """

    insert_time = """
        SELECT DISTINCT measure_date, extract(hour from measure_date), extract(day from measure_date), extract(week from measure_date), 
               extract(month from measure_date), extract(year from measure_date)
        FROM staging_table
    """

    get_locations = """
    SELECT location_id, lat, lon FROM location_dim_table
    """