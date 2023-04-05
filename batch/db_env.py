# db_env.py - print export commands for setting up postgresql database environment for psql

# NOTE: be cautious about using PGPASSWORD if on a multi-user system; others may be able to see 
# your program's environment variables

import json
# read the database information from the json file
with open('../dbconn.json', 'r') as f:
    di = json.load(f)

# print the shell commands to execute so that psql can run effortlessly
print (f"export PGDATABASE={di['database']}")
print (f"export PGHOST={di['host']}")
print (f"export PGPORT={di['port']}")
print (f"export PGUSER={di['username']}")
print (f"export PGPASSWORD={di['password']}")
