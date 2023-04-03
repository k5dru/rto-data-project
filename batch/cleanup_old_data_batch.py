#!/usr/bin/env python
# coding: utf-8

# ### Prerequisite:  
# * miniconda installation from 2023 or later 
# * in anaconda powershell prompt, "conda activate 2023"

# In[1]:


import pandas as pd 

from datetime import datetime
import pytz

    
import json
# read the database information from the json file
with open('../dbconn.json', 'r') as f:
    di = json.load(f)
# create a connection string for postgresql
pg_uri = f"//{di['username']}:{di['password']}@{di['host']}:{di['port']}/{di['database']}"


# In[5]:


#!pip install sqlalchemy
#!pip install psycopg2

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
con    = alchemyEngine.connect();
# con.execute (text("SET default_tablespace = u02_pgdata"))
con.execute (text("create schema if not exists sppdata authorization current_user"))
con.execute (text("set search_path to sppdata"))

con.autocommit=True;
# Read data from PostgreSQL database table and load into a DataFrame instance
#dataFrame       = pd.read_sql(text("select * from information_schema.tables"), con);
#dataFrame

def pgsqldf(query): 
    return pd.read_sql(text(query), con)


# In[6]:


def space(): 
    return pgsqldf("""
    SELECT
      nspname || '.' || C.relname AS "relation",
      pg_total_relation_size(C.oid) AS "total_size",
      pg_relation_size(C.oid) AS "data_size",
    --  pg_total_relation_size(C.oid) / pg_relation_size(C.oid) AS "bloat_ratio",
      pg_stat_user_tables.n_live_tup AS "row_count",
      pg_stat_user_tables.n_dead_tup AS "dead_rows"
    FROM pg_class C
    LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
    LEFT JOIN pg_stat_user_tables ON (pg_stat_user_tables.relid = C.oid)
    WHERE nspname NOT IN ('pg_catalog', 'information_schema')
      AND C.relkind <> 'i'
      AND nspname !~ '^pg_toast'
      AND nspname = 'sppdata'
      and pg_total_relation_size(C.oid) > 0
    ORDER BY pg_total_relation_size(C.oid) DESC
    """)

print(space())


# In[7]:


# perform vacuum; run space again 
con.execute (text("vacuum"))

print(space())


# In[8]:


def trim_table(table, timekey, delete_older_than): 
    df = pgsqldf(f"""
        select '{table}' as table, 
        count(*) as rowcount, 
        count(case when {timekey} < current_timestamp - interval '{delete_older_than}' then 1 else null end) as old_row_count
        from {table}
    """)
    print (df)
    con.execute (text(f"""
        delete from {table} where {timekey} < current_timestamp - interval '{delete_older_than}' 
        """))
    con.execute (text(f""" 
        vacuum (analyze) {table}
        """))
    


# In[9]:


for table, timekey in ( 
     ['sppdata.rtbm_lmp_by_location', 'gmtinterval_end'],
     ['sppdata.tie_flows_long', 'gmttime'],
     ['sppdata.da_lmp_by_location', 'gmtinterval_end'],
     ['sppdata.rtbm_binding_constraints', 'gmtinterval_end'],
     ['sppdata.area_control_error', 'gmttime'],
     ['sppdata.generation_mix', 'gmt_mkt_interval'],
     ['sppdata.stlf_vs_actual', 'gmtinterval_end'],
     ['sppdata.mtlf_vs_actual', 'gmtinterval_end']):
    trim_table(table, timekey, '2 weeks')


# In[10]:


print(space())


# In[12]:


# perform vacuum; run space again 
con.execute (text("vacuum"))

print(space())


# In[ ]:




