# requirements: 
#!pip install sqlalchemy
#!pip install psycopg2

import pandas as pd 
    
import json
# read the database information from the json file
with open('../dbconn.json', 'r') as f:
    di = json.load(f)
# create a connection string for postgresql
pg_uri = f"//{di['username']}:{di['password']}@{di['host']}:{di['port']}/{di['database']}"


# using https://pythontic.com/pandas/serialization/postgresql as example
# though https://naysan.ca/2020/05/31/postgresql-to-pandas/ avoids the sqlalchemy layer
# Example python program to read data from a PostgreSQL table
# and load into a pandas DataFrame
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text

# Create an engine instance
alchemyEngine   = create_engine(
    f'postgresql+psycopg2:{pg_uri}', pool_recycle=3600
).execution_options(isolation_level="AUTOCOMMIT");

 # Connect to PostgreSQL server
con = alchemyEngine.connect();
con.autocommit=False;

# Read data from PostgreSQL database table and load into a DataFrame instance
df = pd.read_sql(text("select * from information_schema.tables"), con);
print (df)
