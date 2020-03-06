import os
import pandas as pd
import sqlalchemy as sql
import dash_bootstrap_components as dbc


# Define global variables
DBUSER = os.getenv('POSTGRES_USER')
DBNAME = os.getenv('DATA_DB')
DBPASS = os.getenv('POSTGRES_PASSWORD')
QUERY_TOP_FIVE_SONGS = """
    SELECT b.song_id, b.title, a.total_repros FROM (
        SELECT COUNT(*) AS total_repros, song_id FROM songplays
        GROUP BY song_id
        ORDER BY total_repros DESC
        LIMIT 5) AS a
    JOIN songs b ON a.song_id = b.song_id
    ORDER BY a.total_repros DESC;
"""

QUERY_TOP_FIVE_ARTISTS = """
    SELECT b.artist_id, b.name, a.total_repros FROM (
        SELECT COUNT(*) AS total_repros, artist_id FROM songplays
        GROUP BY artist_id
        ORDER BY total_repros DESC
        LIMIT 5) AS a
    JOIN artists b ON a.artist_id = b.artist_id
    ORDER BY a.total_repros DESC;
"""

QUERY_TOP_FIVE_USERS = """
    SELECT b.user_id, b.first_name, b.last_name, a.total_repros FROM (
        SELECT COUNT(*) AS total_repros, user_id FROM songplays
        GROUP BY user_id
        ORDER BY total_repros DESC
        LIMIT 5) AS a
    JOIN users b ON a.user_id = b.user_id
    ORDER BY a.total_repros DESC;
"""

AVERAGE_PLAYS = """
    SELECT AVG(a.plays) as Average_Plays_Per_Session FROM (
        SELECT session_id, COUNT(*) as plays FROM songplays
    GROUP BY session_id) AS a;
"""

GENDER_COUNT = """
    SELECT gender, COUNT(*) as total
    FROM users
    GROUP BY gender;
"""

# Get SQL engine from sqlalchemy
engine = sql.create_engine(
    f'postgresql+psycopg2://{DBUSER}:{DBPASS}@db:5432/{DBNAME}'
    )


def generate_table(dataframe):
    """
    Generate a table representing the result of the query

    :param dataframe: a Pandas.DataFrame with the result of the query
    :return: a dash table for the frontend
    """
    return dbc.Table.from_dataframe(
        dataframe,
        striped=True,
        bordered=True,
        hover=True
    )


def get_query(dropdown_input):
    """
    Retrieves a query regarding the input of the dropdown

    :param dropdown_input: option selected by the user
    :return: sql's query to perform and show as text
    """
    query = ''
    if dropdown_input == 'USR':
        query = QUERY_TOP_FIVE_USERS
    elif dropdown_input == 'LVL':
        query = GENDER_COUNT
    elif dropdown_input == 'AVG':
        query = AVERAGE_PLAYS
    elif dropdown_input == 'ART':
        query = QUERY_TOP_FIVE_ARTISTS
    elif dropdown_input == 'SNG':
        query = QUERY_TOP_FIVE_SONGS
    return query


def query_db(query):
    """
    Performs a query to the database bringing a dataframe as a result

    :param query: SQL's query to perform
    :return: pandas.DataFrame with the query result
    """
    df = pd.read_sql_query(
        sql=query,
        con=engine,
    )
    return df
