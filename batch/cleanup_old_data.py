#!/usr/bin/env python
# coding: utf-8

# Goal:  for all the tables populated by the spp_fetch_data program, remove rows from the table older than a specific time interval. For example, it may be desirable to keep 2 weeks of history, so this program removes rows older than two weeks.
# 
# The enviroment is the same as fetch_spp_data.ipynb; see details in that notebook for setting up the software environment.
# 
# This notebook may be downloaded as python and scheduled to run periodically. 
# 

# In[1]:


import pandas as pd 

from datetime import datetime
import pytz


# Read database credentials from a json file. To create the json file, edit "sample_dbconn.py" and run it; all the other programs in this repo will ready dbconn.json for credentials
# 

# In[2]:


import json
# read the database information from the json file
with open('../dbconn.json', 'r') as f:
    di = json.load(f)
# create a connection string for postgresql
pg_uri = f"//{di['username']}:{di['password']}@{di['host']}:{di['port']}/{di['database']}"


# In[3]:


import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text

# Create an engine instance
alchemyEngine   = create_engine(
    f'postgresql+psycopg2:{pg_uri}', pool_recycle=3600
).execution_options(isolation_level="AUTOCOMMIT");

 # Connect to PostgreSQL server
con    = alchemyEngine.connect();

con.execute (text("create schema if not exists sppdata authorization current_user"))
con.execute (text("set search_path to sppdata"))


# In[4]:


con.autocommit=True;

# define a very simple function to run a query and reutrn a dataframe
def pgsqldf(query): 
    return pd.read_sql(text(query), con)


# In[5]:


# define a function to print a space report for objects on the database in the "sppdata" namespace.
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

space()


# In[6]:


# perform vacuum; run space again 
con.execute (text("vacuum"))

space()


# In[7]:


# define a function that will accept a table name, a column name (ideally indexed), and an interval of time for which to keep data.
# example:  trim_table('area_control_error', 'gmttime', '2 weeks')

# Uses the existing connection "con".  Should rewrite all this using object oriented methodolgy.

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
    


# In[8]:


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


# In[9]:


space()


# In[10]:


# perform vacuum; run space again 
con.execute (text("vacuum"))

print (space())


# In[ ]:




