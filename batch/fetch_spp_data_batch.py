#!/usr/bin/env python
# coding: utf-8

# # goal:  fetch the minimal set of data from SPP sources for the current interval or a past interval
# # persist in a database (try postgresql)
# 
# 
#  ## TODO
#  - DONE: figure out if we are ON or OFF the renaming of columns bandwagon.  We are ON the rename bandwagon for now, but a change to standardize_columns would change that.
#      Background: https://dba.stackexchange.com/questions/250943/should-i-not-use-camelcase-in-my-column-names
#  
#  - REFACTOR to get the most recent available RTBM file, and use that as a time basis. 
#      * Combine as much as possible in a common function 
#      * Backfill: Provide the ability to query recent intervals that are not in the database, create a valid path from interval timestamps (stlf appears tricky), and slowly backfill a day of data
#      * Optional Forward fill: for Multi-Day Resource Assessment, provide a way to forward-fill for a few days
#  - CLEAN UP so that when exported to a python file it just runs without edits, and produces sane reports from each module

# ### Prerequisite:  
# * miniconda installation from 2023 or later 
# * in anaconda powershell prompt, "conda activate 2023"

# In[1]:


#!pip install duckdb
import pandas as pd 

from datetime import datetime
import pytz


# Source data model is in https://docs.google.com/spreadsheets/d/1Qh28Lb4dcbw9YMqcXLSj7N8l6Tlr46xNQkV-t1A2txc/edit#gid=0
# 

# ## Define local database. Duckdb is cool and all, but how about a real database? 
# 
#     to initiate session:
#     SET default_tablespace = u02_pgdata;
#     create schema if not exists sppdata authorization current_user;
#     set search_path to sppdata;
#     
#     to drop everything: 
#      select 'drop table ' || table_name || ' cascade;' from information_schema.tables where table_schema = 'sppdata';
# 
# 

# In[2]:
import json
# read the database information from the json file
with open('../dbconn.json', 'r') as f:
    di = json.load(f)
# create a connection string for postgresql
pg_uri = f"//{di['username']}:{di['password']}@{di['host']}:{di['port']}/{di['database']}"


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
alchemyEngine   = create_engine(f'postgresql+psycopg2:{pg_uri}', pool_recycle=3600);

 # Connect to PostgreSQL server
con    = alchemyEngine.connect();
# con.execute (text("SET default_tablespace = u02_pgdata"))
con.execute (text("create schema if not exists sppdata authorization current_user"))
con.execute (text("set search_path to sppdata"))

con.autocommit=False;
# Read data from PostgreSQL database table and load into a DataFrame instance
#dataFrame       = pd.read_sql(text("select * from information_schema.tables"), con);
#dataFrame
 

def pgsqldf(query): 
    return pd.read_sql(text(query), con)


# ### example dataframe to table 
# 
#     dataFrame.to_sql("test_table", con=con, if_exists='replace', index=False);

# ### example table to dataframe: 
#     df=pd.read_sql(text("select * from information_schema.tables"), con)

# In[4]:


con.commit();  #  OH YEAH!  I'm on a real database!  changes need to be committed.


# In[5]:


# if wanting to nuke the world and start over: 
if False: 
    for table_name in ['rtbm_lmp_by_location', 
        'da_lmp_by_location', 
        'area_control_error', 
        'stlf_vs_actual', 
        'mtlf_vs_actual', 
        'tie_flows', 
        'rtbm_binding_constraints',
        'generation_mix',
        'settlement_location',
    ]: 
        con.execute(text(f"drop table {table_name} cascade"))
 


# In[6]:


# define a function to transform source data column names to a more appropriate form for working with data:
def standardize_columns(df): 
    df.columns = (df.columns
                    .str.replace('^ ', '', regex=True)
                    .str.replace('(?<=[a-z])(?=[A-Z])', '_', regex=True)
                    .str.replace('[_ ]+', '_', regex=True)
                    .str.lower()
                 )    
    
    # add an inserted time to all dataframes
    if not 'inserted_time' in df.columns.values: 
        df['inserted_time'] = datetime.now(pytz.timezone("America/Chicago"))

     
    print("standardized columns: ",  df.columns.values)


# # Settlement Locations
# and their estimated locations, from a local project file

# In[7]:



#     # per https://duckdb.org/docs/guides/python/import_pandas.html, duckdb just knows about dataframes; no import necessary
#     # create the table "my_table" from the DataFrame "my_df"
#     duckdb.sql("CREATE or replace TABLE settlement_location AS SELECT * FROM df")
#     # or
#     # insert into the table "my_table" from the DataFrame "my_df"
#     # duckdb.sql("INSERT INTO my_table SELECT * FROM my_df")
# 
#     # check that it got there
#     duckdb.sql("SELECT table_catalog, table_name, column_name, is_nullable, data_type from information_schema.columns")

# In[8]:


if False:    # this only needs to be done when settlement locations table changes, and now views depend on it.

    df.to_sql("settlement_location", con=con, if_exists='replace', index=False); 

    con.execute(text("""alter table settlement_location 
    add constraint settlement_location_pk 
    primary key (settlement_location)"""
                    )
               )

    con.commit()  #  OH YEAH!  I'm on a real database!  changes need to be committed.
  


# In[9]:


# select sample data for fun
pd.read_sql(text("SELECT * from settlement_location order by random() limit 10"), con)


# ### todo: Bug: if first append works but primary key fails to create, nothing else will work ever.

# In[10]:


def pg_insertnew(table_name, primary_keys, df, con):
    # insert df into table_name but only if those rows aren't already there
    
    # make sure the target table exists, but empty (by iloc[0:0])
    try: 
        print ("pg_insertnew: trying append", table_name, con)
        con.commit() # maybe it needs no outstanding transactions before a DDL?  I think that was it
        df.to_sql(table_name, con=con, if_exists='append', index=False); 
        try_merge=False
    except: 
        print ("pg_insertnew append failed, rolling back")
        con.rollback();
        try_merge=True

        print ("pg_insertnew making sure table has PK")

    # make sure the target table has a primary key(s)
    con.execute(text(f"""
    DO $$
    BEGIN
        if NOT exists (
          select constraint_name
          from information_schema.table_constraints
          where table_name = '{table_name}' 
          and constraint_type = 'PRIMARY KEY'
        ) then ALTER TABLE {table_name}
          ADD CONSTRAINT {table_name}_pk PRIMARY KEY ({','.join(primary_keys)});
        end if;
    end $$"""))
    
    con.commit();

    if try_merge: 
#        print ("trying merge")
        print (f"pg_insertnew loading {table_name}_stg")
        # load df to a stage table
        df.to_sql(f"{table_name}_stg", con=con, if_exists='replace', index=False); 
        con.commit();

        # Insert new rows into permanent table
        joinstring=''
        for k in primary_keys: 
            if k == primary_keys[0]: 
                joinstring += f's.{k} = g.{k}'
            else:
                joinstring += f' and s.{k} = g.{k}'

        print (f"pg_insertnew loading {table_name}_stg to {table_name}")
        con.execute(text(f"""
           insert into {table_name}
           select s.* from {table_name}_stg s
           left join {table_name} g
           on {joinstring}
           where g.{primary_keys[0]} IS NULL
        """))

        con.commit();
        con.execute (text(f"""drop table {table_name}_stg"""))
        con.commit();    
                     
    return True


# # Generation Mix
# 
# ## todo: handle web server errors like 
# - IncompleteRead: IncompleteRead(4044 bytes read, 2 more expected)
# 

# In[11]:


# try something harder: 2 hour generation mix. 

def update_generation_mix(con):
    df=pd.read_csv("https://marketplace.spp.org/file-browser-api/download/generation-mix-historical?path=%2FGenMix2Hour.csv", 
                   parse_dates=['GMT MKT Interval'], 
                   infer_datetime_format = True)
   
    standardize_columns(df)

    pg_insertnew(table_name='generation_mix', primary_keys=['gmt_mkt_interval'], df=df, con=con)
    
    return pgsqldf("select * from generation_mix order by gmt_mkt_interval desc limit 5")

# update_generation_mix(con)


# In[12]:


# test query, just because I can:

pgsqldf("""
    select gmt_mkt_interval, 
     gmt_mkt_interval at time zone 'America/Chicago' as local_interval,
    coal_market+coal_self as coal,
    diesel_fuel_oil_market+diesel_fuel_oil_self as diesel,
    hydro_market+hydro_self as hydro,
    natural_gas_market+natural_gas_self as natural_gas,
    nuclear_market+nuclear_self as nuclear,
    solar_market+solar_self as solar,
    wind_market+wind_self as wind,
    waste_disposal_services_market+waste_disposal_services_self
      +waste_heat_market+waste_heat_self
      +other_market+other_self as other,
    load 
    from generation_mix
    order by gmt_mkt_interval desc
    limit 10
""")


# # RTBM LMP by Settlement Location
# 
# Depends on:  
# generation_mix table, for the most recent interval. 
# Will be stored in the ci (current interval) dataframe.
# 
# ### TODO
#  * determine if file names change with DST, and what the duplicate hour in November looks like
#  * switch to grabbing that "latest interval" file
#      * on FTP at pubftp.spp.org/Markets/RTBM/LMP_By_SETTLEMENT_LOC/RTBM-LMP-SL-latestInterval.csv 
#      * on HTTPS at https://marketplace.spp.org/file-browser-api/download/rtbm-lmp-by-location?path=%2FRTBM-LMP-SL-latestInterval.csv
#  
# 
# 

# In[13]:


def get_current_interval(): 
    retdf = pgsqldf("""
    with c as (
        select max(gmt_mkt_interval) at time zone 'America/Chicago' as interval_cpt
        from generation_mix
    )
    , intervalmunge as (
        select interval_cpt, 
        '1970-01-01 00:00:00'::timestamp + (interval '5 minutes' * (floor(extract(EPOCH from interval_cpt)::numeric / 300.0) + 1)) as interval_end_cpt,
        '1970-01-01 00:00:00'::timestamp + (interval '1 hour' * (floor(extract(EPOCH from interval_cpt)::numeric / 3600.0) + 1)) as hour_end_cpt,
        '1970-01-01 00:00:00'::timestamp + (interval '1 hour' * (floor(extract(EPOCH from (interval_cpt + interval '5 minutes'))::numeric / 3600.0) + 1)) as pathhour_end_cpt  
        from c
    )
    select 
/*     date_part('year', interval_end_cpt)::char as rt_yyyy,
    lpad(date_part('month', interval_end_cpt)::char, 2, '0') as rt_mm,
    lpad(date_part('day', interval_end_cpt)::char, 2, '0') as rt_dd,
    lpad(date_part('hour', interval_end_cpt)::char, 2, '0') as rt_hh24,
    lpad(date_part('minute', interval_end_cpt)::char, 2, '0') as rt_mi,
*/
    to_char(interval_end_cpt, 'YYYY') as rt_yyyy,
    to_char(interval_end_cpt, 'MM') as rt_mm,
    to_char(interval_end_cpt, 'DD') as rt_dd,
    to_char(interval_end_cpt, 'HH24') as rt_hh24,
    to_char(interval_end_cpt, 'MI') as rt_mi,
    
    to_char(hour_end_cpt, 'YYYY') as da_yyyy,
    to_char(hour_end_cpt, 'MM') as da_mm,
    to_char(hour_end_cpt, 'DD') as da_dd,
    to_char(hour_end_cpt, 'HH24') as da_hh24,
    to_char(pathhour_end_cpt, 'HH24') as pathda_hh24
    
    from intervalmunge
    """)
    return retdf

con.rollback()
get_current_interval()


# In[14]:


def update_rtbm_lmp(con):
    # Pull out of generation_mix the most recent interval, in a format needed to get other information: 
    ci = get_current_interval()
    
    rt_yyyy=ci.rt_yyyy.values[0]
    rt_mm  =ci.rt_mm.values[0]
    rt_dd  =ci.rt_dd.values[0]
    rt_hh24  =ci.rt_hh24.values[0]
    rt_mi  =ci.rt_mi.values[0]

    # if this interval exists already in rtbm_lmp_by_location, skip the rest
    try: 
        rtbm_db_df=pgsqldf(f"""
        select *
        from rtbm_lmp_by_location
        where (gmtinterval_end at time zone 'America/Chicago') = '{rt_yyyy}-{rt_mm}-{rt_dd} {rt_hh24}:{rt_mi}:00'
        limit 5
        """)

        assert len(rtbm_db_df.index) > 0

        return rtbm_db_df
            
    except: 
        get_rtbm=True

    print ("updating RTBM data from current interval:", get_rtbm)

    fpath=f"https://marketplace.spp.org/file-browser-api/download/rtbm-lmp-by-location?" + \
          f"path=%2F{rt_yyyy}%2F{rt_mm}%2FBy_Interval%2F{rt_dd}%2F" + \
          f"RTBM-LMP-SL-{rt_yyyy}{rt_mm}{rt_dd}{rt_hh24}{rt_mi}.csv"

# todo:  handle 404 gracefully.  Sometimes the RTBM doesn't solve. 

    print (f"reading {fpath}")

    dfnew=pd.read_csv(fpath, parse_dates=['GMTIntervalEnd'], 
                   infer_datetime_format = True)

    """
    dfnew.rename(columns={'Interval':'interval', 'GMTIntervalEnd':'gmt_interval_end', 'Settlement Location':'settlement_location',
                   'Pnode':'pnode', 'LMP':'lmp', 'MLC':'mlc', 'MCC':'mcc', 'MEC':'mec'}, inplace=True)
    """ 
#   in this source file, GMTIntervalEnd does not have a timezone.  Add it: 
    from datetime import timezone
    dfnew['GMTIntervalEnd'] = dfnew['GMTIntervalEnd'].dt.tz_localize(timezone.utc)

    standardize_columns(dfnew)
    
# interval is now redundant 
    dfnew.drop(axis='columns', columns=['interval'], inplace=True)

#    return dfnew

    # insert rows that don't already exist
    
    pg_insertnew('rtbm_lmp_by_location', ['gmtinterval_end', 'settlement_location'], dfnew, con)
        
    con.commit()    
    
    rtbm_db_df=pgsqldf(f"""
       SELECT * 
       from rtbm_lmp_by_location 
       order by gmtinterval_end desc, random() limit 5
    """)
    return rtbm_db_df   

#con.rollback()
#update_rtbm_lmp(con)


# # DA LMP by Settlement Location
# 
# 

# # TODO: fix the test in RTBM and DALMP that used 'interval' to determine if it needed to reload. 

# In[15]:


def update_da_lmp(con):
    # Pull out of generation_mix the most recent interval, in a format needed to get other information: 
    ci = get_current_interval()
    
    da_yyyy=ci.da_yyyy.values[0]
    da_mm  =ci.da_mm.values[0]
    da_dd  =ci.da_dd.values[0]
    da_hh24  =ci.da_hh24.values[0]

        # if this interval exists already in da_lmp_by_location, skip the rest
    try: 
        da_db_df=pgsqldf(f"""
        select *
        from da_lmp_by_location
        where (gmtinterval_end at time zone 'America/Chicago') = '{da_yyyy}-{da_mm}-{da_dd} {da_hh24}:00:00'
        limit 5
        """)

        assert len(da_db_df.index) > 0

        return da_db_df
    except: 
        get_da=True

    print ("updating DA data from current interval:", get_da)

    fpath=f"https://marketplace.spp.org/file-browser-api/download/da-lmp-by-location?" + \
          f"path=%2F{da_yyyy}%2F{da_mm}%2FBy_Day%2FDA-LMP-SL-{da_yyyy}{da_mm}{da_dd}0100.csv"
        
    print (f"reading {fpath}")

    try: 
        # these are big; if I've already run once today it is cached
        dfnew=pd.read_pickle(f"DA-LMP-SL-{da_yyyy}{da_mm}{da_dd}0100.pickle")
        print (f"read local cached version DA-LMP-SL-{da_yyyy}{da_mm}{da_dd}0100.pickle")
    except: 
        dfnew=pd.read_csv(fpath, parse_dates=['GMTIntervalEnd'], 
                   infer_datetime_format = True)
        dfnew.to_pickle(f"DA-LMP-SL-{da_yyyy}{da_mm}{da_dd}0100.pickle")
        print (f"saved local cached version DA-LMP-SL-{da_yyyy}{da_mm}{da_dd}0100.pickle")
    """
    dfnew.rename(columns={'Interval':'interval', 'GMTIntervalEnd':'gmt_interval_end', 'Settlement Location':'settlement_location',
                   'Pnode':'pnode', 'LMP':'lmp', 'MLC':'mlc', 'MCC':'mcc', 'MEC':'mec'}, inplace=True)
    """
    
    #   in this source file, GMTIntervalEnd does not have a timezone.  Add it: 
    from datetime import timezone
    dfnew['GMTIntervalEnd'] = dfnew['GMTIntervalEnd'].dt.tz_localize(timezone.utc)

    standardize_columns(dfnew)
# interval is now redundant 
    dfnew.drop(axis='columns', columns=['interval'], inplace=True)

    
    # insert rows that don't already exist
    pg_insertnew('da_lmp_by_location', ['gmtinterval_end', 'settlement_location'], dfnew, con)
        
    con.commit()    
    
    da_db_df=pgsqldf(f"""
       SELECT * 
       from da_lmp_by_location 
       order by gmtinterval_end desc, random() limit 5
       """)
    return da_db_df   

#con.rollback()
#update_da_lmp(con)


# # Area Control Error
#     ftp://pubftp.spp.org/Operational_Data/ACE/ACE.csv

# In[16]:


def update_ace(con):
    table_name="area_control_error"
    source_url="ftp://pubftp.spp.org/Operational_Data/ACE/ACE.csv"
    primary_keys=['gmttime']
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTTime'], 
                   infer_datetime_format = True
                  )
    
    print (df.columns.values)
    
    # df.rename(columns={'GMTTime':'gmt_time', 'Value':'value'}, inplace=True)
    standardize_columns(df)  
    print(df.columns)
    
    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
    
    return pgsqldf(f"select * from {table_name} order by gmttime desc limit 5")

#update_ace(con)


# # STLF vs. Actual
#     https://marketplace.spp.org/file-browser-api/download/stlf-vs-actual?path=%2F2023%2F02%2F25%2F15%2FOP-STLF-202302251435.csv
#     
# ## TODO:  this one is strange; is there a file with more data?  If not, 
# ## need to figure out how replace older rows with newer ones while keeping ones that have already aged out
# 
# DONE - maybe delete rows with NULLs in Actual column before inserting new values?  If done in one transaction 
#     * already done in Tie Flows; just do that
# - need to remove commits from the pg_insertnew to keep client from seeing missing data between delete and insert
# 
# - at 23:30, tried to read
# https://marketplace.spp.org/file-browser-api/download/stlf-vs-actual?path=/2023/03/01/23/OP-STLF-202303012330.csv
# - but it is not there; latest file is pubftp.spp.org/Operational_Data/STLF/2023/03/01/00/OP-STLF-202303012330.csv
# 
# ## fix start-of-hour 404: 
# ## at about 11:03, this was 404: 
#     #  https://marketplace.spp.org/file-browser-api/download/stlf-vs-actual?path=/2023/03/02/11/OP-STLF-202303021100.csv
#     # found here
#     #  https://marketplace.spp.org/file-browser-api/download/stlf-vs-actual?path=/2023/03/02/12/OP-STLF-202303021100.csv
#     # so the value of da_hh24 in the path had already advanced, 5 minutes early
#     

# In[17]:


def update_stlf(con):
    
    # Pull out of generation_mix the most recent interval, in a format needed to get other information: 
    ci = get_current_interval()
    
    rt_yyyy=ci.rt_yyyy.values[0]
    da_yyyy=ci.da_yyyy.values[0]
    rt_mm  =ci.rt_mm.values[0]
    da_mm  =ci.da_mm.values[0]
    rt_dd  =ci.rt_dd.values[0]
    da_dd  =ci.da_dd.values[0]
    rt_hh24  =ci.rt_hh24.values[0]
    da_hh24  =ci.da_hh24.values[0]
    pathda_hh24  =ci.pathda_hh24.values[0]
    rt_mi  =ci.rt_mi.values[0]
    
    table_name="stlf_vs_actual"
    
    source_url=f"https://marketplace.spp.org/file-browser-api/download/stlf-vs-actual?" + \
               f"path=%2F{rt_yyyy}%2F{rt_mm}%2F{rt_dd}%2F{pathda_hh24}%2FOP-STLF-{rt_yyyy}{rt_mm}{rt_dd}{rt_hh24}{rt_mi}.csv"
    primary_keys=['gmtinterval_end']
    
    print ("reading", source_url)
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTInterval'], 
                   infer_datetime_format = True
                  )

    # fix this one error - end was left off of this table's timestamp
    df.rename(columns={'GMTInterval':'GMTIntervalEnd'}, inplace=True)

    print (df.columns.values)
    print (df)
        
#   in this source file, GMTIntervalEnd does not have a timezone.  Add it: 
    from datetime import timezone
    df['GMTIntervalEnd'] = df['GMTIntervalEnd'].dt.tz_localize(timezone.utc)
        
    standardize_columns(df)  
# interval is now redundant 
    df.drop(axis='columns', columns=['interval'], inplace=True)

    con.commit();
    
    try:
        con.execute(text("""delete from stlf_vs_actual where actual is null""")); 
        con.commit(); 
    except: 
        con.rollback();

    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
        
    return pgsqldf(f"select * from {table_name} order by gmtinterval_end desc limit 5")


#update_stlf(con)


# # MTLF Vs. Actual
#     https://marketplace.spp.org/file-browser-api/download/mtlf-vs-actual?path=%2F2023%2F02%2F25%2FOP-MTLF-202302251600.csv
#     
# ## Todo:  
# DONE same thing as STLF re null values
# - this is kind of big and doesn't update but once an hour; maybe cache the file?
#     

# In[18]:


def update_mtlf(con):
    
    # Pull out of generation_mix the most recent interval, in a format needed to get other information: 
    ci = get_current_interval()
    
    rt_yyyy=ci.rt_yyyy.values[0]
    da_yyyy=ci.da_yyyy.values[0]
    rt_mm  =ci.rt_mm.values[0]
    da_mm  =ci.da_mm.values[0]
    rt_dd  =ci.rt_dd.values[0]
    da_dd  =ci.da_dd.values[0]
    rt_hh24  =ci.rt_hh24.values[0]
    da_hh24  =ci.da_hh24.values[0]
    rt_mi  =ci.rt_mi.values[0]
    
    table_name="mtlf_vs_actual"
    source_url=f"https://marketplace.spp.org/file-browser-api/download/mtlf-vs-actual?" + \
               f"path=%2F{rt_yyyy}%2F{rt_mm}%2F{rt_dd}%2FOP-MTLF-{rt_yyyy}{rt_mm}{rt_dd}{rt_hh24}00.csv"
    primary_keys=['gmtinterval_end']
    
    
    # if this interval exists already in the database, don't do this update
    try: 
        test_df=pgsqldf(f"""
        select *
        from mtlf_vs_actual
        where gmtinterval_end = '{da_yyyy}-{da_mm}-{da_dd} {da_hh24}:00:00'
        and averaged_actual is NOT NULL
        limit 5
        """)

        assert len(test_df.index) > 0

        print (f"update_mltf: found '{da_yyyy}-{da_mm}-{da_dd} {da_hh24}:00:00' already in database")
        return test_df
            
    except: 
        print (f"update_mltf: gmtinterval_end '{da_yyyy}-{da_mm}-{da_dd} {da_hh24}:00:00' not yet in database")
        get_rtbm=True
    
    
    print ("reading", source_url)
    
    # this file is not huge but consider caching it locally instead of reading from remote 12 times an hour
    # or, better, test the database to see if we need to update it.
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTIntervalEnd'], 
                   infer_datetime_format = True
                  )
        
    # df.rename(columns={'Interval':'interval', 'GMTIntervalEnd':'gmt_interval_end', 'MTLF':'mtlf', 'Averaged Actual':'averaged_actual'}, inplace=True)
    
    #   in this source file, GMTIntervalEnd does not have a timezone.  Add it: 
    from datetime import timezone
    df['GMTIntervalEnd'] = df['GMTIntervalEnd'].dt.tz_localize(timezone.utc)
    
    standardize_columns(df)  

    # interval is now redundant 
    df.drop(axis='columns', columns=['interval'], inplace=True)

    
    try:
        con.execute(text("""delete from mtlf_vs_actual where averaged_actual is null""")); 
        con.commit(); 
    except: 
        con.rollback();
        

    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
        
    return pgsqldf(f"select * from {table_name} where averaged_actual is not null order by gmtinterval_end desc limit 5")

#update_mtlf(con)



# # Tie Flows
#     ftp://pubftp.spp.org/Operational_Data/TIE_FLOW/TieFlows.csv
#     
# ## TODO: 
# - Implement periodic vacuum from all these deletes
# 
# - Completely refactor to convert wide-form data to long-form data.  That would avoid completely breaking this interface if an area is added, removed or renamed.
#     * AND it would completely fix the delete problem by not inserting NULL values in the first place. 

# In[19]:


## DEPRECATED fragile wide-form; if the layout changes, we have to redo everything.  See now update_tie_flows_long. 
def update_tie_flows(con):
    table_name="tie_flows"
    source_url="ftp://pubftp.spp.org/Operational_Data/TIE_FLOW/TieFlows.csv"
    primary_keys=['gmttime']
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTTime'], 
                   infer_datetime_format = True
                  )
    
    #df.rename(columns={'GMTTime':'gmt_time'}, inplace=True)
    standardize_columns(df)
    
    try:
        con.execute(text("""delete from tie_flows where spp_nsi is null"""))
        con.commit() 
    except: 
        con.rollback()
        
    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
    
    return pgsqldf(f"""select * from {table_name} where spp_nsi is not null order by gmttime desc limit 5""")




# In[20]:


#con.rollback()

def update_tie_flows_long(con):
    table_name="tie_flows_long"
    source_url="ftp://pubftp.spp.org/Operational_Data/TIE_FLOW/TieFlows.csv"
    primary_keys=['gmttime', 'area']
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTTime'], 
                   infer_datetime_format = True
                  )
    
    df = pd.melt(df, id_vars=['GMTTime'], ignore_index=True).dropna()
    
    df.rename(columns={'GMTTime':'gmttime', 'variable':'area', 'value':'mw'}, inplace=True)
    
    standardize_columns(df)  # also adds inserted_time
    
    # remove future values that will be replaced
    #try:
    con.execute(text(f"""
      delete from "{table_name}" where area = 'SPP NSI Future'
      and gmttime > current_timestamp
      """))
    con.commit() 
    #except: 
    #    con.rollback()
    
    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
    
    return pgsqldf(f"""select * from {table_name} 
    where gmttime between current_timestamp - interval '2 minutes' and current_timestamp + interval '2 minutes'
    order by random() limit 5""")


#df = update_tie_flows_long(con)
#con.commit()

#df


# # Real-Time Binding Constraints
#     https://marketplace.spp.org/file-browser-api/download/rtbm-binding-constraints?path=%2FRTBM-BC-latestInterval.csv
#     
#  ## TODO
#  DONE: figure out if we are ON or OFF the renaming of columns bandwagon - we are currently ON

# In[21]:


def update_rt_binding(con):
    table_name="rtbm_binding_constraints"
    source_url="https://marketplace.spp.org/file-browser-api/download/rtbm-binding-constraints?path=%2FRTBM-BC-latestInterval.csv"
    primary_keys=['gmtinterval_end', 'constraint_name']
    
    df=pd.read_csv(source_url, 
                   parse_dates=['GMTIntervalEnd'], 
                   infer_datetime_format = True
                  )
    
    #print (df[['GMTIntervalEnd','Constraint Name','Constraint Type','NERCID','Monitored Facility']])

    #   in this source file, GMTIntervalEnd does not have a timezone.  Add it: 
    from datetime import timezone
    df['GMTIntervalEnd'] = df['GMTIntervalEnd'].dt.tz_localize(timezone.utc)

    # RT binding constraints file has dupes; fix it here
    df.drop_duplicates(subset=None, keep='first', inplace=True, ignore_index=False)
    
    """ df.rename(columns={'Interval':'interval', 'GMTIntervalEnd':'gmt_interval_end', 
                       'Constraint Name':'constraint_name', 'Constraint Type':'constraint_type',
                       'NERCID':'nercid', 'TLR Level':'tlr_level', 'State':'state', 'Shadow Price':'shadow_price',
                       'Monitored Facility':'monitored_facility', 'Contingent Facility':'contingent_facility'}, inplace=True)
    """
    standardize_columns(df)
    
    # interval is now redundant 
    df.drop(axis='columns', columns=['interval'], inplace=True)

#    con.execute(text("""delete from rtbm_binding_constraints where "SPP NSI" is null""")); 
#    con.commit(); 
      
    pg_insertnew(table_name=table_name, primary_keys=primary_keys, df=df, con=con)
    
    con.commit()
    
    return pgsqldf(f"""select * from {table_name} order by gmtinterval_end desc limit 5""")

#update_rt_binding(con)


# # DONE.  
# ### Below here is just calling it again to make sure that works.
# 

# In[22]:


if True: 
    update_generation_mix(con)
    update_ace(con)
    update_rtbm_lmp(con)
    update_da_lmp(con)
    update_stlf(con)
    update_mtlf(con)
    #remove: update_tie_flows(con)
    update_tie_flows_long(con)
    update_rt_binding(con)
    con.commit()


# In[23]:


from time import sleep
while False:
    sleep(300)
    try: 
        update_generation_mix(con)
        update_ace(con)
        update_rtbm_lmp(con)
        update_da_lmp(con)
        update_stlf(con)
        update_mtlf(con)
        update_tie_flows(con)
        update_rt_binding(con)
        con.commit()    
    except: 
        con.rollback()
        


# In[24]:


con.commit()


# In[ ]:




