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

# **This notebook is to create a table system id**

# CELL ********************

#import libraries
import yaml


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declare variables
# yaml_path = "/lakehouse/default/Files/config/yaml/control/etl_run_log.yaml"
# yaml_base_path = "/lakehouse/default/Files/config/yaml/bronze/"
yaml_path = yaml_base_path+yaml_file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#read yaml file
with open(yaml_path, "r") as yaml_files:
    yaml_config = yaml.safe_load(yaml_files)
# print(yaml_config)
# print(yaml_config["table"])
    table_name = yaml_config["table"]["name"]
    table_schema_name = yaml_config["table"]["schema_name"]
    table_columns = yaml_config["table"]["columns"]
    table_column_name = ",\n ".join([f'{table_column["name"]} {table_column["data_type"]}' for table_column in table_columns])
    # print(table_column_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

create_table_sql = f"""
create table if not exists {table_schema_name}.{table_name}({table_column_name})
"""
# print(create_table_sql)
spark.sql(create_table_sql)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- %%sql
# create schema control
# create schema bronze;
# create schema silver;
# create schema gold


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# -- %%sql
# -- DROP TABLE control.etl_run_log

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
