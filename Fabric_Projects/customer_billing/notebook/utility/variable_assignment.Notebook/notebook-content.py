# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "17e8e705-e1c0-4eb3-801a-483f8ff5391a",
# META       "default_lakehouse_name": "lk_customer_billing",
# META       "default_lakehouse_workspace_id": "c4eb058f-0e68-4d26-84de-d6d954df390b",
# META       "known_lakehouses": [
# META         {
# META           "id": "17e8e705-e1c0-4eb3-801a-483f8ff5391a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# This note book we use to create a variable from dev.config 

# CELL ********************

#import libraries
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#decalring variable
file_location = "/lakehouse/default/Files/config/dev.config.json"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%fs ls "Files/config"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# open json file
with open(file_location) as f:
    config_data = json.load(f)
# print(config_data) 
# declaring variable 
env = config_data["env"]
input_folder = config_data["input_folder"]
unzip_folder = config_data["unzip_folder"]
excepted_files = config_data["excepted_files"]
# print(env)
# print(input_folder)
# print(unzip_folder)
# print(excepted_files)   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(env, input_folder, unzip_folder, excepted_files)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
