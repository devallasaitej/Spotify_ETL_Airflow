import pandas as pd 
import requests
from datetime import datetime
import datetime
import pandas as pd 
import requests
from datetime import datetime
import datetime


TOKEN = "YOUR_TOKEN_HERE"
print('started')
# Creating an function to be used in other pyrhon files
def return_dataframe(): 
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
     

    # Download all songs you've listened to "after yesterday", which means in the last 24 hours      
    r = requests.get("https://api.spotify.com/v1/artists/1mYsTxnqsietFxj1OgoGbG/albums?limit=50&offset=0", headers = input_variables)

    data = r.json()
    album_type = []
    total_tracks = []
    album_name = []
    release_date = []
    artists = []
    album_id = []
    spotify_uri = []
    

    for item in data['items']:
        album_type.append(item['album_type'])
        total_tracks.append(item['total_tracks'])
        album_id.append(item['id'])
        album_name.append(item['name'])
        release_date.append(item['release_date'])
        spotify_uri.append(item['uri'])
        artst_list = []
        for artist in item['artists']:
            artst_list.append(artist['name'])
        artists.append('#'.join(artst_list))

    album_dict = {
        'album_type':album_type,
        'total_tracks':total_tracks,
        'album_id':album_id,
        'album_name':album_name,
        'release_date':release_date,
        'spotify_uri':spotify_uri,
        'artists' : artists
    }

    album_df = pd.DataFrame(album_dict, columns=['album_type','total_tracks','album_name','release_date','artists','album_id','spotify_uri'])
    return album_df

def Data_Quality(load_df):
    #Checking Whether the DataFrame is empty
    if load_df.empty:
        print('No Albums Extracted')
        return False
    
    #Enforcing Primary keys since we don't need duplicates
    if pd.Series(load_df['album_id']).is_unique:
       pass
    else:
        #The Reason for using exception is to immediately terminate the program and avoid further processing
        raise Exception("Primary Key Exception,Data Might Contain duplicates")
    
    #Checking for Nulls in our data frame 
    if load_df.isnull().values.any():
        raise Exception("Null values found")
    
def Transform_df(load_df):

    # load_df['release_date'] = pd.to_datetime(load_df['release_date'])
    # load_df['release_year'] = load_df['release_date'].dt.year

    Transformed_df = load_df.copy()

    return Transformed_df

def spotify_etl():
    #Importing the songs_df from the Extract.py
    load_df=return_dataframe()
    Data_Quality(load_df)
    #calling the transformation
    Transformed_df=Transform_df(load_df)    
    print(load_df)
    return (load_df)

spotify_etl()