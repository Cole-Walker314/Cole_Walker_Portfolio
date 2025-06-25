'''
Fleet Foxes Tracks Airflow DAG

DESCRIPTION:
This DAG extracts track data from the spotify api, transforms the data into a pandas DataFrame,
and loads the table into a local PostgresSQL database that is provided by the Airflow Astro CLI. The
table created in Postgres is then loaded in the FleetFoxesSpotifyReport to be visualized.
'''

from pendulum import datetime
from airflow.sdk import DAG, Context, Variable
from airflow.operators.python import PythonOperator
import requests
import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def get_track_data(**context: Context):
    '''
    This function extracts the data from Fleet Foxes' first five albums, using the 
    https://api.spotify.com/v1/albums spotify endpoint. Using the extracted albums, the function then
    iterating on the track ids found in the albums to retrieve track data using the
    https://api.spotify.com/v1/tracks endpoint.
    '''

    # Defining the POST request to receive the api token using Spotify's provided Client Credentials
    res = requests.post(
        'https://accounts.spotify.com/api/token',
        data={'grant_type' : 'client_credentials'},
        auth=(Variable.get('spotify_client_id'), Variable.get('spotify_client_secret'))
    )

    response_data = res.json()
    token = response_data['access_token']

    # Defining the GET request to extract JSON output using access token
    FF_albums = {'Fleet Foxes': '5GRnydamKvIeG46dycID6v','Helplessness Blues':'7D0rCfJjFj9x0bdgRKtvzb','Crack-Up':'0xtTojp4zfartyGtbFKN3v','Shore':'0lmjCPEcec2k6L7ysNIcd3', 'A Very Lonely Solstice' : '28aerKYZtxvFfNflCyE29h'}
    album_values = FF_albums.values()
    album_url = f"https://api.spotify.com/v1/albums?market=US&ids={','.join(album_values)}"

    get_several_albums = requests.get(
        album_url,
        headers={'Authorization' : f"Bearer {token}"}
    )

    track_ids = []

    album_json = get_several_albums.json()
    album_data = album_json['albums']

    for album in album_data:
        for track in album['tracks']['items']:
            track_ids.append(track['id'])

    track_url = f"https://api.spotify.com/v1/tracks?market=US&ids={','.join(track_ids)}"

    track_response = requests.get(
        track_url,
        headers={'Authorization' : f"Bearer {token}"}
    )

    track_json = track_response.json()

    context['ti'].xcom_push(key="json_data", value=track_json)

def export_track_data(**context: Context):
    '''
    This function transforms and exports the data retrieved in the previous 'get__data' function 
    into a local PostgresSQL database as a pandas DataFrame.
    '''

    track_json = context['ti'].xcom_pull(key="json_data", task_ids="get_all_tracks_data_task")

    track_normalized_data = pd.json_normalize(
    data=track_json['tracks'],
    record_path=['artists'],
    meta=[
        'id',
        'name',
        'popularity',
        'track_number',
        'duration_ms',
        'explicit',
        ['album','id'],
        ['album','release_date'],
        ['external_urls', 'spotify' ]
        ],
    meta_prefix='tracks_',
    record_prefix='artists_'
    )

    logging.info(track_normalized_data)
    
    pg_hook = PostgresHook(postgres_conn_id='spotify_dag_to_pg')
    pg_engine = pg_hook.sqlalchemy_url

    logging.info(f"Created PG Hook URI: {pg_engine}")

    sql_engine = create_engine(pg_engine)

    logging.info(f"Created SQL Engine: {sql_engine}")

    track_normalized_data.to_sql(
        name='spotify_all_tracks',
        con=sql_engine,
        schema='dev',
        if_exists='replace',
        index=False
    )

with DAG(
    dag_id="fleet_foxes_all_tracks_dag",
    description="This DAG brings track data from Spotify into a local postgres db using Spotify's web API",
    schedule='@daily',
    catchup=False,
    tags=["Spotify", "API Call", "Postgres"]
):

    get_spotify_data_task = PythonOperator(
        task_id="get_all_tracks_data_task",
        python_callable=get_track_data
    )

    export_spotify_data_task = PythonOperator(
        task_id="export_all_tracks_data_task",
        python_callable=export_track_data
    )

get_spotify_data_task >> export_spotify_data_task