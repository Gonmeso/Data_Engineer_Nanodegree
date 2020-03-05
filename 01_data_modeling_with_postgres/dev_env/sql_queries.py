# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays
    (songplay_id SERIAL,
    start_time timestamp,
    user_id varchar NOT NULL,
    level varchar,
    song_id varchar,
    artist_id varchar,
    session_id int NOT NULL,
    location varchar,
    user_agent varchar,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id));
    """)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users
    (user_id varchar,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender varchar NOT NULL,
    level varchar NOT NULL);
    """)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs
    (song_id varchar,
    title varchar NOT NULL,
    artist_id varchar,
    year int NOT NULL,
    duration numeric NOT NULL,
    PRIMARY KEY(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id));
    """)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists
    (artist_id varchar,
    name varchar NOT NULL,
    location varchar,
    latitude numeric,
    longitude numeric,
    PRIMARY KEY(artist_id));
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
    weekday int);
    """)

# INSERT RECORDS

songplay_table_insert = (
    """
    INSERT INTO songplays (start_time, user_id,
                           level, session_id,
                           location, user_agent,
                           song_id, artist_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name,
                       gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id)
    DO NOTHING;
    """)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, artist_id,
                           year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (song_id)
    DO NOTHING;
    """)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location,
                        latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id)
    DO NOTHING;
    """)


time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day,
                      week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """)

# FIND SONGS

song_select = (
    """
    SELECT a.song_id, b.artist_id FROM songs a, artists b
    WHERE a.title = %s
    AND b.name = %s
    AND a.duration = %s
    """)

# BULK COPY

bulk_copy = (
    """
    COPY {} FROM '{}' DELIMITER ',' CSV HEADER
    """)

# QUERY LISTS

create_table_queries = [
    user_table_create,
    artist_table_create,
    song_table_create,
    time_table_create,
    songplay_table_create
    ]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
    ]
