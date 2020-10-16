import pandas as pd
import os

# PASS = os.environ['DB_PASS']
# USER = os.environ['DB_USER']
# DBNAME = os.environ['DB_NAME']
# HOST = os.environ['DB_URL']

def ping_postgres():
    test = ['hello', 'world']
    test_pandas = pd.DataFrame([test], columns=['what', 'who'])
    print(test_pandas)


ping_postgres()