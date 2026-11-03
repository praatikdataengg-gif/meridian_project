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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_table = "gold.customers"

print("Target table:", target_table)

display(
    spark.sql(f"DESCRIBE DETAIL {target_table}")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql(f"DESCRIBE HISTORY {target_table}")
    .select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics"
    )
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.catalog.refreshTable(target_table)

display(
    spark.table(target_table)
    .select(
        "customer_id",
        "city",
        "inserted_date",
        "updated_date"
    )
    .orderBy("customer_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history_df = spark.sql("""
    DESCRIBE HISTORY gold.customers
""")

display(
    history_df
    .filter("version IN (7, 8, 9)")
    .select(
        "version",
        "timestamp",
        "operation",
        "operationParameters",
        "userName",
        "job",
        "notebook",
        "operationMetrics"
    )
    .orderBy("version")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.sql("""
        SELECT
            customer_id,
            city,
            inserted_date,
            updated_date
        FROM gold.customers
        ORDER BY customer_id
    """)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    DELETE FROM gold.customers
    WHERE customer_id = 1004
""")

print("Customer 1004 deleted successfully")

spark.sql("""
    UPDATE gold.customers
    SET city = 'TEST'
    WHERE customer_id = 1001
""")

print("Customer 1001 updated successfully")

display(
    spark.table("gold.customers")
    .select(
        "customer_id",
        "city",
        "inserted_date",
        "updated_date"
    )
    .orderBy("customer_id")
)

display(
    spark.sql("DESCRIBE HISTORY gold.customers")
    .select("version", "timestamp", "operation", "operationParameters")
    .limit(5)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_version_7 = (
    spark.read
    .option("versionAsOf", 7)
    .table("gold.customers")
)

display(
    gold_version_7
    .select(
        "customer_id",
        "city",
        "inserted_date",
        "updated_date"
    )
    .orderBy("customer_id")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("gold.customers")
    .select(
        "customer_id",
        "city",
        "inserted_date",
        "updated_date"
    )
    .orderBy("customer_id")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM gold.customers
# MAGIC ORDER BY customer_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM silver.customers
# MAGIC ORDER BY customer_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     city,
# MAGIC     inserted_date,
# MAGIC     updated_date
# MAGIC FROM gold.customers
# MAGIC ORDER BY customer_id;%%sql
# MAGIC 
# MAGIC SELECT
# MAGIC     customer_id,
# MAGIC     city,
# MAGIC     inserted_date,
# MAGIC     updated_date
# MAGIC FROM gold.customers
# MAGIC ORDER BY customer_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
