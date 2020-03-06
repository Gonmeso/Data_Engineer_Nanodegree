import os
import glob
import functools
import logging
import psycopg2
import math
import pandas as pd
from sql_queries import *

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
    )


def clean_nan_data(values):
    for i, v in enumerate(values):
        if isinstance(v, float):
            values[i] = None if math.isnan(v) else v
    return values
         


def process_artists(df, cur):
    """
    Process artist data from a dataframe of songs and artist data

    :param df: a pandas DataFrame containing artist data
    :param cur: cursor to handle postgres insertions
    """
    # insert artist record
    artist_data = list(df[[
        'artist_id',
        'artist_name',
        'artist_location',
        'artist_latitude',
        'artist_longitude'
        ]].values[0])

    cur.execute(artist_table_insert, clean_nan_data(artist_data))


def process_songs(df, cur):
    """
    Process song data from a dataframe of songs and artist data

    :param df: a pandas DataFrame containing song data
    :param cur: cursor to handle postgres insertions
    """
    # insert song record
    song_data = list(df[[
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration'
        ]].values[0])
    cur.execute(song_table_insert, clean_nan_data(song_data))


def process_song_file(cur, filepath):
    """
    Process a json from a filepath and handle the insertion of artist and song
    data into a postgres database

    :param cur: cursor to handle postgres insertions
    :param filepath: filepath where the json to handle is placed
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # Make year a NaN for later processing
    df.year[df.year == 0] = None

    # Process data
    process_artists(df, cur)
    process_songs(df, cur)


def process_time(df, cur):
    """
    Process time data from the applications logs data to generate new 
    attributes and insert them into a postgres database

    :param df: a pandas DataFrame containing the time data
    :param cur: cursor to handle postgres insertions
    """
    # convert timestamp column to datetime
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_df = pd.DataFrame()
    time_df['timestamp'] = df['ts'].astype(str)
    time_df['hour'] = df['ts'].dt.hour
    time_df['day'] = df['ts'].dt.day
    time_df['week'] = df['ts'].dt.week
    time_df['month'] = df['ts'].dt.month
    time_df['year'] = df['ts'].dt.year
    time_df['weekday'] = df['ts'].dt.dayofweek

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))


def process_users(df, cur):

    """
    Process user data from the applications logs data 
    and insert them into a postgres database

    :param df: a pandas DataFrame containing the users data
    :param cur: cursor to handle postgres insertions
    """
    # load user table and drop duplicates
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
        ]].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)


def process_songplays(df, cur):
    """
    Process songplay data from the applications logs data and 
    query existing tables to gather songs and artists information
    to insert them in the songplays table

    :param df: a pandas DataFrame containing the time data
    :param cur: cursor to handle postgres insertions
    """
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = row[[
            'ts',
            'userId',
            'level',
            'sessionId',
            'location',
            'userAgent'
            ]].tolist() + [songid, artistid]
        cur.execute(songplay_table_insert, songplay_data)


def process_log_file(cur, filepath):
    """
    Process a json from a filepath and handle the insertion of time, users
    and songplays data into a postgres database

    :param cur: cursor to handle postgres insertions
    :param filepath: filepath where the json to handle is placed
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page']=='NextSong']

    # Process data from log files
    process_time(df, cur)
    process_users(df, cur)
    process_songplays(df, cur)



def process_data(cur, conn, filepath, func):
    """
    Retrieves all the json files contained in the specified filepath
    and process the data using the specified function

    :param cur: cursor to handle postgres insertions
    :param conn: postgres database connection
    :param filepath: filepath where the json to handle is placed
    :param func: which function to execute
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logging.info('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        logging.info('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=db dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()