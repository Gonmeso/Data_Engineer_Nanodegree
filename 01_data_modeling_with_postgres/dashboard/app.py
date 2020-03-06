# -*- coding: utf-8 -*-
import os
import dash
import pandas as pd
import sqlalchemy as sql
import dash_core_components as dcc
import dash_html_components as html

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
DBUSER = os.getenv('POSTGRES_USER')
DBNAME = os.getenv('POSTGRES_DB')
DBPASS = os.getenv('POSTGRES_PASSWORD')


engine = sql.create_engine('postgresql://{DBUSER}:{DBPASS}@db:5432/{DBNAME}')
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

colors = {
    'background': '#111111',
    'text': '#7FDBFF'
}

engine = sql.create_engine('postgresql://usr:pass@localhost:5432/sqlalchemy')

def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

def get_top_five(table):
    pass


app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Data Modeling With Postgres',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Dash: A web application framework for Python.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    # Query selector
        html.Label('Select Query'),
    dcc.Dropdown(
        id='query-dropdown',
        options=[
            {'label': 'Top 5 Users', 'value': 'USR'},
            {'label': 'Top 5 Artists', 'value': 'ART'},
            {'label': 'Top 5 Songs', 'value': 'SNG'},
            {'label': 'Number of Songplays', 'value': 'SNP'},
            {'label': 'Level', 'value': 'LVL'}
        ],
        placeholder = 'Select a query',
        value=[]
    ),
    # Generate dataframe

    # Generate result table

])

if __name__ == '__main__':
    app.run_server(debug=True, host= '0.0.0.0')