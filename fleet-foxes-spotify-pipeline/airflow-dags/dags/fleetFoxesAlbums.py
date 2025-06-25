'''
Fleet Foxes Album Airflow DAG

DESCRIPTION:
This DAG extracts album data from the spotify api, transforms the data into a pandas DataFrame,
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

def get_album_data(**context: Context):
    '''
    This function extracts the data from Fleet Foxes' first five albums, using the 
    https://api.spotify.com/v1/albums spotify endpoint.
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

    spotify_data = get_several_albums.json()

    context['ti'].xcom_push(key="json_data", value=spotify_data)

    logging.info(spotify_data)

def export_album_data(**context: Context):
    '''
    This function transforms and exports the data retrieved in the previous 'get_album_data' function 
    into a local PostgresSQL database as a pandas DataFrame.
    '''

    spotify_data = context['ti'].xcom_pull(key="json_data", task_ids="get_album_data_task")

    album_normalized_data = pd.json_normalize(
        spotify_data['albums'],
        record_path=['images'],
        meta=['id',
              'name',
              'total_tracks',
              'album_type',
              'release_date',
              'popularity',
              'label'
              ],
        meta_prefix='albums_',
        record_prefix='image_'
    )

    df_distinct = album_normalized_data.drop_duplicates(subset='albums_id')

    logging.info(album_normalized_data)
    
    pg_hook = PostgresHook(postgres_conn_id='spotify_dag_to_pg')
    pg_engine = pg_hook.sqlalchemy_url

    logging.info(f"Created PG Hook URI: {pg_engine}")

    sql_engine = create_engine(pg_engine)

    logging.info(f"Created SQL Engine: {sql_engine}")

    df_distinct.to_sql(
        name='spotify_albums',
        con=sql_engine,
        schema='dev',
        if_exists='replace',
        index=False
    )

with DAG(
    dag_id="fleet_foxes_albums_dag",
    description="This DAG brings album data from Spotify into a local postgres db using Spotify's web API",
    schedule='@daily',
    catchup=False,
    tags=["Spotify", "API Call", "Postgres"]
):

    get_album_data_task = PythonOperator(
        task_id="get_album_data_task",
        python_callable=get_album_data
    )

    export_album_data_task = PythonOperator(
        task_id="export_album_data_task",
        python_callable=export_album_data
    )

get_album_data_task >> export_album_data_task