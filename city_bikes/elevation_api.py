import os
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import time


# Database variabler
PASS ="malin"
USER_NAME ="student_malin"
HOST ="ds-etl-academy.cgbivchwjzle.eu-west-1.rds.amazonaws.com"
DATABASE = "gp_camaca"


# # Loading file and making subset
# df_okt= pd.read_csv('10.csv')
# df=df_okt[['start_station_id','start_station_name','start_station_latitude','start_station_longitude']].copy()

# # Group-by to get individual columns
# group= df.groupby(by=['start_station_id','start_station_name','start_station_latitude','start_station_longitude']).count().reset_index()



def get_elevation(df, lat_col, lon_col):
    
    api=[]
    elevation=[]
    print(len(df.index))
    i = 0
    for lat, lon in zip(df[lat_col], df[lon_col]):
        time.sleep(1.5)
        print(i)
        i+=1
        API= requests.get(f'https://api.opentopodata.org/v1/test-dataset?locations={lat},{lon}', timeout=5)
        print(API)
        try:
            data=API.json()
            print(data['results'])
            api.append(data['results'])  
        except:
            pass

    for idx, el in enumerate(api):
        print(idx)
        elevation.append(el[0]['elevation'])  
            
    return elevation, api



def get_locations_legacy():
    URL_LEG_LOC = 'https://data-legacy.urbansharing.com/legacy_station_locations.csv'
    resource = requests.get(URL_LEG_LOC)
    # data = pd.read_csv(resource.content)
    parse = str(resource.content).split('\\n')
    data_leg = pd.DataFrame()
    rows = []
    for row in parse[1:-1]:
        rows.append(row.split(','))
    data_leg = pd.DataFrame(rows, columns=['leg_id', 'latitude', 'longitude'])

    OBS_ROOT= "https://data-legacy.urbansharing.com/legacy_new_station_id_mapping.csv"
    resource = requests.get(OBS_ROOT)
    
    parse = str(resource.content).split('\\n')
    data = pd.DataFrame()
    rows = []
    for row in parse[1:-1]:
        rows.append(row.split(','))
    data = pd.DataFrame(rows, columns=['new', 'leg_id'])

    data_leg = data_leg.merge(data,how='inner', on=['leg_id'])
    
    # # Create engine for db-connection:
    # engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
    # data.to_sql('new_legacy_table', engine, index=False, schema='bysykkel', if_exists='replace', method='multi', chunksize=1000)
    # print("Sending data to db!")
    
    return data_leg


# Testing function:
group = get_locations_legacy()
testing,hoyde= get_elevation(group,'latitude','longitude')

# Creating and concating df
e=pd.DataFrame(testing,columns=['elevation'])
df_station=pd.concat([group, e],axis=1)

# Sending to sql-db:
engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
print('Sending')
df_station.to_sql('all_elevation', engine, index=False, schema='bysykkel',
                  if_exists='replace', method='multi', chunksize=1000)






