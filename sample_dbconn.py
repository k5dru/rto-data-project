#!/usr/bin/env python
# coding: utf-8

# create dbconn.json
di={'username':'your-username',
    'password':'your-vry-long-and-complex-password', 
        'host':'ec2-or-something-maybe.compute-1.amazonaws.com', 
        'port':'5432',
    'database':'some-database-name' }

# write di to a local json file dbconn.json (not to be saved in repo)
import json
with open('dbconn.json', 'w') as f:
    json.dump(di, f)

# use this in your project files:
# 
#  
import json
# read the database information from the json file
with open('dbconn.json', 'r') as f:
    di = json.load(f)
# create a connection string for postgresql
pg_uri = f"//{di['username']}:{di['password']}@{di['host']}:{di['port']}/{di['database']}"

