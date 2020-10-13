import os
import psycopg2
import requests
import pandas as pd
from sqlalchemy import create_engine


# Database variabler
PASS = os.environ['PASS_P']
USER_NAME = os.environ['USER_NAME_P']
HOST = os.environ['HOST_P']
DATABASE = os.environ['DB_TEAM']


date= "2020/09.json"

def get_month_to_db(date, tablename):
    '''Function to load data from website as json, save it as dataframe,
        and send the dataframe to postgres-db'''
    
    OBS_ROOT= "https://data.urbansharing.com/oslobysykkel.no/trips/v1/"
    resource = requests.get(OBS_ROOT + date)
    
    data = resource.json()
    df = pd.DataFrame(data)
    
    # Create engine for db-connection:
    engine = create_engine(f'postgresql+psycopg2://{USER_NAME}:{PASS}@{HOST}/{DATABASE}')
    
    # To db
    try:
        df.to_sql(tablename, engine, index=False, schema='bysykkel', if_exists='replace', chunksize=1000)
        print("Sending data to db!")
        
    except:
        print("Woops!")
        
    return df

# Tester funksjon for å kjøre 1mnd med data til egen postgres-db      
get_month_to_db(date, 'sep_obs')
    