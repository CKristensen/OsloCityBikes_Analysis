import psycopg2
from psycopg2 import sql
import os
import logging

PASS = os.environ['DB_PASS']
USER = os.environ['DB_USER']
DBNAME = os.environ['DB_NAME']
HOST = os.environ['DB_URL']


def connect():
    """Connects to the postgresdatabase
    """    
    conn = psycopg2.connect(host = HOST,
    dbname = DBNAME,
    user = USER,
    password = PASS,
    port = 5432
    )
    cursor = conn.cursor()
    return conn, cursor

def close(conn, cursor):  
    conn.commit()
    cursor.close()
    conn.close()

#Function that initializes our Database Star Schema
#Needs correct env variables set to run properly
def init_star_schema():
    conn, cur = connect()
    with open('db/init_star.sql','r') as sql_file:
        cur.execute(sql_file.read())
    close(conn, cur)
if __name__ == '__main__':
    try:
        init_star_schema()
    except:
        print('Database Already Initialized')