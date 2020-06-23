#!/usr/bin/python
# -*- coding: utf-8 -*-


# This script is written by Anvitha Kandiraju for
# Insight Data Engineering project
# This script reads data from database and creates a web interface




# Import libraries
import dash
import os
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import psycopg2
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
from flask_caching import Cache
import pandas as pd


# Function to connect to PostgresSQL server
# no inputs returns connection

def get_sql_conn():

    # postgres credentials

    pg_user = os.getenv('PG_USER')
    pg_pwd = os.getenv('PG_PW')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')

    # connect to the PostgreSQL database

    conn = psycopg2.connect(host=db_host, port=db_port,
                            database=db_name, user=pg_user,
                            password=pg_pwd)
    return conn


# Function to create readable names for columns in DB
# input argument is coloumn name, output is a human readable text

def getHRparam(param):
    hrparams = {
        'pm25': 'PM 2.5',
        'pm10': 'PM 10',
        'o3': 'Ozone',
        'bc': 'Black carbon',
        'no2': 'Nitrogen dioxide',
        'so2': 'Sulfur dioxide',
        'co': 'Carbon monoxide',
        'surftemp': 'Surface temperature',
        'airtemp': 'Air temperature',
        'rh': 'Relative Humidity',
        'totalprecip': 'Total precipitation',
        'solarrad': 'Daily solar radiation',
        'st20cm': 'Soil temperature',
        'sm20cm': 'Soil moisture',
        }
    return hrparams[param]


# Function to read ZIP code and get latitude and longitude information
# input arguments are zipcode info, outputs are latitude and longitude info

def lat_long_from_zip(zip_code):

    # read csv file that contains zip code information

    df_zip = pd.read_csv('zips.csv')

    # match with user entry to extract latitude and longitude information

    df_row = df_zip[df_zip['ZIP'] == int(zip_code)]

    # if user entry is incorrect, store default number

    if df_row.empty:
        lat = -9999
        lng = -9999
    else:

        lat = df_row['LAT'].values[0]
        lng = df_row['LNG'].values[0]
    return (lat, lng)


# Function to update plot requested by user
# input arguments are latitude, longitude, and parameter selected. outputs are updated plots

def update_plots(lat, lng, param_sel):

    # query to get latitude, longitude for a given parameter, nearest to the user requested coordinates,
    # and distance between them, along with units of the parameter

    query = \
        """ SELECT ST_MakePoint({},{})::geography <-> geom::geography as distance,
                  latitude, longitude, parameter, unit
                FROM {} WHERE parameter=\'{}\' ORDER BY ST_SetSRID(ST_MakePoint({},{}),4326) <-> geom LIMIT 1 """.format(
        str(lng),
        str(lat),
        os.getenv('DB_TABLE'),
        param_sel,
        str(lng),
        str(lat),
        )

    # save result to pandas df

    nearest_result = pd.read_sql_query(query, get_sql_conn())

    # query to get date and average value for a parameter based on nearest result

    dat_query = \
        """ SELECT utc, avg_value FROM {} WHERE parameter=\'{}\' AND latitude={} AND longitude={} ORDER BY utc desc """.format(os.getenv('DB_TABLE'
            ), param_sel, str(nearest_result['latitude'].values[0]),
            str(nearest_result['longitude'].values[0]))

    # save result to pandas df

    scatter_data = pd.read_sql_query(dat_query, get_sql_conn())

    # close connection

    get_sql_conn().close()

    # scatter plot title name and units

    title_name = getHRparam(param_sel)
    unit = nearest_result['unit'].values[0]

    # defining scatter plot

    fig_xyscatter = go.Figure(data=go.Scattergl(x=scatter_data['utc'],
                              y=scatter_data['avg_value'],
                              mode='markers'))

    # update layout for scatter plot

    fig_xyscatter.update_layout(xaxis_title='Date',
                                yaxis_title=str(title_name) + ' ['
                                + str(unit) + ']', title={
        'text': str(title_name) + ' Trend',
        'x': 0.5,
        'y': 0.85,
        'xanchor': 'center',
        'yanchor': 'top',
        }, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')

    # update layout of overview figure to zoom into latitude and longitude user requested

    layout = dict(margin=dict(t=20, b=20, l=20, r=20),
                  font=dict(color='#000000', size=11),
                  paper_bgcolor='#FFFFFF', mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(lat=nearest_result['latitude'].values[0],
                    lon=nearest_result['longitude'].values[0]),
        pitch=0,
        zoom=9,
        style='light',
        ))

    # update overview_figure apply layout changes

    fig_overview_map['layout'] = layout

    # output distance from user requested coordinates to nearest sensor coordinates

    if int(nearest_result['distance'].values[0] / 1000) > 300:
        distance_str = \
            'We could not generate accurate graph. The neanearest_resultt sensor is {} km away'.format(str(int(nearest_result['distance'
                ].values[0] / 1000)))
    else:
        distance_str = \
            'The closest sensor is at a distance of:{} km'.format(str(int(nearest_result['distance'
                ].values[0] / 1000)))

    return (fig_xyscatter, fig_overview_map, distance_str)


# mapbox credentials

mapbox_access_token = os.getenv('MAP_token')

# define style sheet for dash app

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# declaring directory to store cache

cache = Cache(app.server, config={'CACHE_TYPE': 'filesystem',
              'CACHE_DIR': 'cache-directory'})


# cache information in generate_overview_map function
# function to generate overview map data and layout:

@cache.memoize(timeout=6000)
def generate_overview_map():

    # list of parameters to plot on map

    param_list = [
        'pm25',
        'pm10',
        'o3',
        'bc',
        'no2',
        'so2',
        'co',
        'surftemp',
        'airtemp',
        'rh',
        'totalprecip',
        'solarrad',
        'st20cm',
        'sm20cm',
        ]

    # declare empty list of data points

    data = []

    # getting connection to PostgresSQL

    conn = get_sql_conn()

    # for loop to get all unique latitude and longitude info to plot

    for param in param_list:

        # defining data base table name

        db_table = os.getenv('DB_TABLE')

        # definig query to PostgreSQL table

        query = 'SELECT DISTINCT latitude,longitude FROM ' + db_table \
            + ' WHERE parameter = ' + "\'" + param + "\'"

        # read latlong into pandas df

        df_latlon = pd.read_sql_query(query, con=conn)

        # parse coordinates information from query and assign parameter value to each coordinates

        param_data = dict(lat=df_latlon.latitude,
                          lon=df_latlon.longitude,
                          name=getHRparam(param), marker=dict(size=9,
                          opacity=0.5), type='scattermapbox')
        data.append(param_data)

    # bandwidth to other requests

    conn.close()

    # map layout

    layout = dict(margin=dict(t=20, b=20, l=20, r=20),
                  font=dict(color='#000000', size=11),
                  paper_bgcolor='#FFFFFF', mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(lat=40, lon=-105),
        pitch=0,
        zoom=2,
        style='light',
        ))

    # return data and layout for overview map

    return (data, layout)

# read data and layout for the overview map

(data, layout) = generate_overview_map()

# generate overview map

fig_overview_map = go.Figure(data=data, layout=layout)

# Design for the webpage

# color scheme for webpage

colors = {'background': '#FFFFFF', 'text': '#000000'}

# define for the app

app.layout = html.Div(style = {
        'backgroundColor': colors['background']
    },
    children = [html.Div([dbc.Row(dbc.Col(html.Div(html.H2('Inhabit'), style = {
                'color': colors['text'],
                'textAlign': 'center'
            }), width = 12)),
            dbc.Row([dbc.Col(html.Div(dcc.Graph(id = 'map',
                    figure = fig_overview_map)), width = 8),
                dbc.Col([dbc.Row([dbc.Col(html.Div(' Enter US ZIP code', style = {
                            'color': colors['text']
                        })),
                        dbc.Col(html.Div(dcc.Input(id = 'zip',
                                value = 'enter zip', type = 'text', debounce = True),
                            style = {
                                'color': colors['text']
                            })),
                        dbc.Col(html.Button(id = 'submit-zip', n_clicks = 0,
                            children = 'Submit'), style = {
                            'color': colors['text']
                        })
                    ]), dbc.Row(dbc.Col(html.Div('OR'),
                        style = {
                            'color': colors['text']
                        })),
                    dbc.Row([dbc.Col(html.Div('Enter latitude, longitude'), style = {
                            'color': colors['text']
                        }),
                        dbc.Col(html.Div(dcc.Input(id = 'latlong',
                            value = 'enter lat, long', type = 'text',
                            debounce = True), style = {
                            'color': colors['text']
                        })), dbc.Col(html.Button(id = 'submit-latlong',
                                n_clicks = 0, children = 'Submit'),
                            style = {
                                'color': colors['text']
                            })
                    ]),
                    dbc.Row(dbc.Col(html.Div('Select Parameters'),
                        style = {
                            'color': colors['text']
                        }, width = 3)),
                    dbc.Row(dbc.Col(dcc.Dropdown(id = 'parameter-dropdown', options = [{
                        'label': 'PM 2.5',
                        'value': 'pm25'
                    }, {
                        'label': 'PM 10',
                        'value': 'pm10'
                    }, {
                        'label': 'Air temperature',
                        'value': 'airtemp'
                    }, {
                        'label': 'Relative Humidity',
                        'value': 'rh'
                    }, {
                        'label': 'Total precipitation',
                        'value': 'totalprecip'
                    }, {
                        'label': 'Surface temperature',
                        'value': 'surftemp'
                    }, {
                        'label': 'Carbon monoxide',
                        'value': 'co'
                    }, {
                        'label': 'Nitrogen dioxide',
                        'value': 'no2'
                    }, {
                        'label': 'Sulfur dioxide',
                        'value': 'so2'
                    }, {
                        'label': 'Ozone',
                        'value': 'o3'
                    }, {
                        'label': 'Black Carbon',
                        'value': 'bc'
                    }, {
                        'label': 'Soil temperature',
                        'value': 'st20cm'
                    }, {
                        'label': 'Soil moisture',
                        'value': 'sm20cm'
                    }, ], value = 'so2')))
                ], width = 4)
            ])
        ]),
        html.Div([dbc.Row([dbc.Col(html.Div(dcc.Graph(id = 'xyscatter')),
                width = 8),
            dbc.Col(dbc.Row(dbc.Col(html.Div(id = 'dist_output',
                    children = 'Enter zipcode or lat long to calculate distance ', style = {
                        'color': colors['text'],
                        'fontSize': 25
                    }))),
                width = 4)
        ])])
    ])


# call back function to create interactive features of web app
# inputs to this function are zipcode info, coordinates info and parameter selected in drop dow
# outputs from the function are xy scatter plot, overview map and distance to the nearest sensor



@app.callback([Output(component_id='xyscatter',
              component_property='figure'), Output(component_id='map',
              component_property='figure'),
              Output(component_id='dist_output',
              component_property='children')],
              [Input(component_id='parameter-dropdown',
              component_property='value'), Input('submit-zip',
              'n_clicks'), Input('submit-latlong', 'n_clicks')],
              [State(component_id='zip', component_property='value'),
              State(component_id='latlong', component_property='value'
              )])
def update_scatter_plot(
    param_sel,
    zip_button,
    latlong_button,
    zip_code,
    latlong,
    ):

    # define trigger which provides input entered by user

    global trigger

    # define button id to remember input by user

    global button_id

    # define call back context that gives id for input entered by user

    ctx = dash.callback_context

    # prevent exception on loading the page as no user entry is found

    if not ctx.triggered or not ctx.triggered[0]['value']:
        trigger = 'no_clicks'
        raise dash.exceptions.PreventUpdate
    else:

    # if user entered zipcode or coordinates, determine which entry is used

        trigger = ctx.triggered[0]['prop_id'].split('.')[0]
        if trigger == 'submit-latlong' or trigger == 'submit-zip':
            button_id = trigger

    # if zipcode is entered

    if button_id == 'submit-zip':

         # get latititude and longitude information

        (lat, lng) = lat_long_from_zip(zip_code)

        # if latitude and longitude information is incorrect raise exception

        if lat < -90 or lat > 90 or lng < -180 or lng > 180:
            raise dash.exceptions.PreventUpdate

        # if valid coordinates, find nearest sensor and get updated plots

        (fig_xyscatter, fig_overview_map, distance_str) = \
            update_plots(lat, lng, param_sel)
        return (fig_xyscatter, fig_overview_map, distance_str)

    # if latlong is entered

    if button_id == 'submit-latlong':

         # parse to get user latitude and longitude information

        (lat, lng) = latlong.split(',')

        # if latitude and longitude information is incorrect raise exception

        if lat < -90 or lat > 90 or lng < -180 or lng > 180:
            raise dash.exceptions.PreventUpdate

        # if valid coordinates, find nearest sensor and get updated plots

        (fig_xyscatter, fig_overview_map, distance_str) = \
            update_plots(lat, lng, param_sel)

        return (fig_xyscatter, fig_overview_map, distance_str)

    return (fig_xyscatter, fig_overview_map, distance_str)

# main function

if __name__ == '__main__':

# run host on a public server

    app.run_server(debug=True, host='0.0.0.0')