import datetime as dt
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine

from airflow.utils.dates import days_ago
from spotify_etl import spotify_etl

default_args = {
    'owner': 'sdevalla',
    'depends_on_past': False,
    'start_date': dt.today().date(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1)
}

dag = DAG(
    'Spotify_ETL_DAG',
    default_args=default_args,
    description='Spotify ETL process 1-min',
    schedule_interval=dt.timedelta(minutes=50),
)

def ETL():
    print("started")
    df=spotify_etl()
    #print(df)
    conn = BaseHook.get_connection('postgre_sql')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    df.to_sql('my_played_tracks', engine, if_exists='replace')

with dag:    
    ext= PostgresOperator(
        task_id='Extract_Transform',
        postgres_conn_id='postgre_sql',
        sql="""
        CREATE TABLE IF NOT EXISTS arr_albums(
        album_type VARCHAR(200),
        total_tracks INT,
        album_name VARCHAR(200),
        release_date VARCHAR(200),
        artists VARCHAR(500),
        album_id VARCHAR(500),
        spotify_uri VARCHAR(500),
        CONSTRAINT primary_key_constraint PRIMARY KEY (album_id)
        )
        """
    )

    load = PythonOperator(
        task_id='DB_Load',
        python_callable=ETL,
        dag=dag,
    )

    ext >> load


