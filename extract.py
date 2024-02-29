import pandas as pd 
import requests
from datetime import datetime
import datetime

TOKEN = "BQD9N31_brv8B8ppSLwyV5A87qAZ3Yyb7k8ACOiMG1ak87hNoe6fq7pY07ZBR42dfGLLBGxulFfhyndb0VfXdJobxkIX3VUgzldFHTZ2UBVDdbqwr38"


# Creating an function to be used in other pyrhon files
def return_dataframe(): 
    input_variables = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
         
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