
import os
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import time


# Database variabler
PASS = os.environ['PASS_P']
USER_NAME = os.environ['USER_NAME_P']
HOST = os.environ['HOST_P']
DATABASE = os.environ['DB_TEAM']


# Loading file and making subset
df_okt= pd.read_csv('10.csv')
df=df_okt[['start_station_id','start_station_name','start_station_latitude','start_station_longitude']].copy()

# Group-by to get individual columns
group= df.groupby(by=['start_station_id','start_station_name','start_station_latitude','start_station_longitude']).count().reset_index()



def get_elevation(df, lat_col, lon_col):
    
    api=[]
    elevation=[]
    
    for lat, lon in zip(df[lat_col], df[lon_col]):
        API= requests.get(f'https://api.opentopodata.org/v1/test-dataset?locations={lat},{lon}')
        print(API)
        time.sleep(2)
        data=API.json()
        print(data['results'])
        
        api.append(data['results'])  
        
    for idx, el in enumerate(api):
        print(idx)
        elevation.append(el[0]['elevation'])  
            
    return elevation, api

# Testing function:
testing,hoyde= get_elevation(group,'start_station_latitude','start_station_longitude')

# Creating and concating df
e=pd.DataFrame(testing,columns=['elevation'])
df_station=pd.concat([group, e],axis=1)

# Sending to sql-db:
engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
    
df_station.to_sql('elevation', engine, index=False, schema='bysykkel',
                  if_exists='replace', method='multi', chunksize=1000)






