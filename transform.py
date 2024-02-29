import extract
import pandas as pd 
import datetime
from datetime import datetime, date

# Set of Data Quality Checks Needed to Perform Before Loading
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

if __name__ == "__main__":

    load_df= extract.return_dataframe()
    Data_Quality(load_df)
    #calling the transformation
    Transformed_df=Transform_df(load_df)    
    print(Transformed_df)