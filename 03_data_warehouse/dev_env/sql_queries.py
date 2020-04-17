from utils import get_output_str, cfg


ROLE_ARN = get_output_str('SparkifyRoleOutput')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_events
    (event_id integer IDENTITY(0,1),
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    item_in_session integer,
    last_name varchar,
    length numeric,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration bigint,
    session_id integer,
    song varchar,
    status integer,
    start_time timestamp,
    user_agent varchar,
    user_id varchar)
    DISTSTYLE KEY
    DISTKEY (artist)
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs integer,
    artist_id varchar,
    latitude numeric,
    longitude numeric,
    location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year integer)
    DISTSTYLE KEY
    DISTKEY (artist_id);
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id integer IDENTITY(0,1),
    start_time timestamp,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar,
    user_agent varchar,
    PRIMARY KEY(songplay_id))
    DISTSTYLE KEY
    DISTKEY (artist_id);
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users
    (user_id varchar,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar NOT NULL,
    level varchar NOT NULL,
    PRIMARY KEY(user_id))
    DISTSTYLE ALL;
    """)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs
    (song_id varchar,
    title varchar NOT NULL,
    artist_id varchar,
    year int,
    duration numeric NOT NULL,
    PRIMARY KEY(song_id))
    DISTSTYLE ALL;
    """)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric,
    PRIMARY KEY(artist_id))
    DISTSTYLE ALL;
    """)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time
    (start_time timestamp,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int)
    DISTSTYLE ALL;
    """)

# STAGING TABLES

staging_events_copy = \
    """
    COPY staging_events
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    TIMEFORMAT AS 'epochmillisecs'
    REGION '{}'
    JSON '{}'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL;
    """.format(
        cfg['S3']['LOG_DATA'],
        ROLE_ARN,
        cfg['AWS']['REGION'],
        cfg['S3']['LOG_JSONPATH']
    )

staging_songs_copy = \
    """
    COPY staging_songs
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    REGION '{}'
    JSON 'auto'
    TRUNCATECOLUMNS
    BLANKSASNULL
    EMPTYASNULL;
    """.format(
            cfg['S3']['SONG_DATA'],
            ROLE_ARN,
            cfg['AWS']['REGION']
        )

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id,
                           level, session_id,
                           location, user_agent,
                           song_id, artist_id)
    SELECT e.start_time, e.user_id, e.level, e.session_id, e.location,
           e.user_agent, s.song_id, s.artist_id
    FROM staging_events e
    JOIN staging_songs s ON e.artist = s.artist_name
    AND e.song = s.title
    WHERE e.page = 'NextSong';
    """)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name,
                       gender, level)
    SELECT DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL;
    """)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id,
                           year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
    """)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location,
                        latitude, longitude)
    SELECT DISTINCT artist_id, artist_name AS name, location, latitude, longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
    """)


time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day,
                      week, month, year, weekday)
    SELECT DISTINCT start_time,
                    EXTRACT(hour from start_time) as hour,
                    EXTRACT(day from start_time) as day,
                    EXTRACT(week from start_time) as week,
                    EXTRACT(month from start_time) as month,
                    EXTRACT(year from start_time) as year,
                    DATE_PART(WEEKDAY, start_time) AS weekday
    FROM staging_events
    WHERE start_time IS NOT NULL;
    """)

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
    ]
