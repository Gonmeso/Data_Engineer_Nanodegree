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
    (songplay_id int NOT NULL,
    start_time timestamp,
    user_id int NOT NULL,
    level int,
    song_id int NOT NULL,
    artist_id int NOT NULL,
    session_id int NOT NULL,
    location varchar,
    user_agent varchar,
    PRIMARY KEY(songplay_id),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id),
    FOREIGN KEY(start_time) REFERENCES time(start_time));
    """)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users
    (user_id int,
    first_name varchar NOT NULL,
    last_name varchar NOT NULL,
    gender boolean NOT NULL,
    level varchar NOT NULL,
    PRIMARY KEY(user_id));
    """)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs
    (song_id int,
    title varchar NOT NULL,
    artist_id int,
    year int NOT NULL,
    duration numeric NOT NULL,
    PRIMARY KEY(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id));
    """)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists
    (artist_id int,
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
    weekday boolean,
    PRIMARY KEY(start_time));
    """)

# INSERT RECORDS

songplay_table_insert = (
    """
    INSERT INTO songplays (songplay_id, start_time, user_id,
                           level, song_id, artist_id, session_id,
                           location, user_agent)
    VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {})
    """)

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")


time_table_insert = ("""
""")

# FIND SONGS

song_select = ("""
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
