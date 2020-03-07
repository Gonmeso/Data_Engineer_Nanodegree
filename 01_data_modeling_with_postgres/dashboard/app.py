# -*- coding: utf-8 -*-
from utils import get_query, generate_table, query_db
import dash
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
logo = 'https://images.ctfassets.net/2y9b3o528xhq/1I5Q4ulQdaV2Cd2RxJG1NF/9012118676e481a058bbbc1951c953c1/degree-hat.svg'
title = 'Data Modeling With Postgres: A example of simple\
     queries on the fact table'


navbar = dbc.Navbar(
    children=[
        dbc.Row(
            [
                dbc.Col(
                    html.Img(
                        src=logo,
                        height="30px"
                        )
                    ),
                dbc.Col(dbc.NavbarBrand(title, className="ml-2")),
            ],
            align="center",
            no_gutters=True,
        ),
    ],
    color="dark",
    dark=True,
)


query_body = dbc.Row(
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
    ]
)


table_output = html.Div(id='query-table')
app.layout = html.Div(children=[navbar, query_body, table_output])


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
    app.run_server(host='0.0.0.0')
