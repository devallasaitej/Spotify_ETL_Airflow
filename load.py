import extract
import transform
import sqlalchemy
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///albums.sqlite"

if __name__ == "__main__":

#Importing the songs_df from the Extract.py
    load_df=extract.return_dataframe()
    if(transform.Data_Quality(load_df) == False):
        raise ("Failed at Data Validation")
    Transformed_df=transform.Transform_df(load_df)
    #The Two Data Frame that need to be Loaded in to the DataBase

#Loading into Database
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('albums.sqlite')
    cursor = conn.cursor()

    #SQL Query to Create Played Songs
    sql_query_1 = """
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
    cursor.execute(sql_query_1)

    print("Opened database successfully")

    #We need to only Append New Data to avoid duplicates
    try:
        Transformed_df.to_sql("arr_albums", engine, index=False, if_exists='append')
    except Exception as e:
        print("An error occurred while writing data to the database:", e)
  
    #cursor.execute('DROP TABLE arr_albums')

    conn.close()
    print("Close database successfully")
    
    