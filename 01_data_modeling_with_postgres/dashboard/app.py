# -*- coding: utf-8 -*-
import os
import dash
import pandas as pd
import sqlalchemy as sql
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
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

engine = sql.create_engine(f'postgresql+psycopg2://{DBUSER}:{DBPASS}@db:5432/{DBNAME}')
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

def generate_table(dataframe):
    return dbc.Table.from_dataframe(dataframe, striped=True, bordered=True, hover=True)


def get_query(dropdown_input):
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
    df = pd.read_sql_query(
        sql=query,
        con=engine,
    )
    return df

navbar = dbc.Navbar(
    children=[
        dbc.Row(
            [
            dbc.Col(
                html.Img(
                    src='//images.ctfassets.net/2y9b3o528xhq/1I5Q4ulQdaV2Cd2RxJG1NF/9012118676e481a058bbbc1951c953c1/degree-hat.svg',
                    height="30px"
                    )
                ),
            dbc.Col(dbc.NavbarBrand("Data Modeling With Postgres: A example of simple queries on the fact table", className="ml-2")),
            ],
    align="center",
    no_gutters=True,
        ),
    ],
    color="dark",
    dark=True,
)


app.layout = html.Div(children=[
    navbar,

    # Query selector
    dbc.Row(
        [
            dbc.Col(
                dbc.Select(
                    id='query-dropdown',
                    options=[
                        {'label': 'Top 5 Users', 'value': 'USR'},
                        {'label': 'Top 5 Artists', 'value': 'ART'},
                        {'label': 'Top 5 Songs', 'value': 'SNG'},
                        {'label': 'Average songplays for session', 'value': 'AVG'},
                        {'label': 'Level', 'value': 'LVL'}
                    ],
                )
            ),
            dbc.Col(
                html.Div(
                    id='query-text',
                    style={
                        'textAlign': 'center',
                    }
                )
            )
        ],
    ),
    # Generate dataframe
    html.Div(id='query-table'),
    # Generate result table

])

@app.callback(
    Output(component_id='query-text', component_property='children'),
    [Input('query-dropdown', 'value')])
def show_query(input_value):
    return get_query(input_value)


@app.callback(
    Output(component_id='query-table', component_property='children'),
    [Input('query-dropdown', 'value')])
def show_table(input_value):
    if input_value:
        return generate_table(query_db(get_query(input_value)))

if __name__ == '__main__':
    app.run_server(debug=True, host= '0.0.0.0')