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

%run "./common_function"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

run_start_time = datetime.now()
yaml_path = "/lakehouse/default/Files/config/yaml/silver/"
dummy_date = "9999-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# #Generate hash key
# def generate_hash_key(dataFrame):
#     hash_payload = F.to_json(
#         F.struct(*[
#             F.col(c).cast("string").alias(c) for c in ytable_hash_key_columns
#         ]), options={"ignoreNullFields": "false"}
#     )
#     return dataFrame.withColumn("hash_key", F.sha2(hash_payload, 256))
def generate_hash_key(dataFrame):
    hash_payload = F.to_json(
        F.struct(*[F.col(c).cast("string").alias(c) for c in ytable_hash_key_columns]),
        options={"ignoreNullFields": "false"}
    )
    return dataFrame.withColumn("hash_key", F.sha2(hash_payload, 256))


def get_delta_version(table_name):
    return (spark.sql(f"DESCRIBE HISTORY {table_name}")
                 .agg(F.max("version")).collect()[0][0])


def get_merge_counts(table_name, start_version):
    rows = (spark.sql(f"DESCRIBE HISTORY {table_name}")
                 .filter((F.col("version") > start_version) & (F.col("operation") == "MERGE"))
                 .select("operationMetrics").collect())
    inserted = sum(int(r["operationMetrics"].get("numTargetRowsInserted", 0)) for r in rows)
    updated  = sum(int(r["operationMetrics"].get("numTargetRowsUpdated", 0)) for r in rows)
    return inserted, updated

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

all_files = [f for f in mssparkutils.fs.ls("Files/config/yaml/silver/")
             if f.name.lower().endswith((".yaml", ".yml"))]
if not all_files:
    raise FileNotFoundError(f"No YAML files found in {yaml_path}")

failed_tables = []

for actual_file_path in all_files:
    run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    absolute_path = yaml_path + actual_file_path.name
    table_name = actual_file_path.name.split(".")[0]

    # defaults so the except block never hits a NameError
    final_table_name = table_name
    inserted_count = 0
    updated_count = 0
    start_version = None

    try:
        # ---------- Read YAML configuration ----------
        reading_yaml_data = open_file_read_data(absolute_path)
        ytable_name = reading_yaml_data["table"]["name"]
        ytable_schema_name = reading_yaml_data["table"]["schema_name"]
        ytable_source_schema_name = reading_yaml_data["table"]["source_schema_name"]
        final_source_table_name = ytable_source_schema_name + "." + ytable_name
        final_table_name = ytable_schema_name + "." + ytable_name
        ytable_columns_names = [col["name"] for col in reading_yaml_data["table"]["columns"]]
        ytable_business_key = reading_yaml_data["table"]["business_key"]
        ytable_hash_key_columns = reading_yaml_data["table"]["hash_key"]["columns"]

        # ---------- START LOG ----------
        process_log("Start", table_name=table_name, layer="silver")
        metadata_columns = ["hash_key", "inserted_date", "updated_date", "is_latest"]
        data_columns = [c for c in ytable_columns_names if c not in metadata_columns]

        # ---------- Validations ----------
        if not ytable_business_key:
            raise ValueError(f"No business key in {final_table_name}")
        if not ytable_hash_key_columns:
            raise ValueError(f"No hash key columns in {final_table_name}")
        if not spark.catalog.tableExists(final_source_table_name):
            raise ValueError(f"Source table not found: {final_source_table_name}")
        if not spark.catalog.tableExists(final_table_name):
            raise ValueError(f"Target table not found: {final_table_name}")

        # ---------- Bronze: read, duplicate check, hash ----------
        df_bronze = spark.table(final_source_table_name).select(*data_columns)
        if df_bronze.groupBy(*ytable_business_key).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Duplicate business keys found in Bronze - {final_source_table_name}")
        temp_hash_bronze = generate_hash_key(df_bronze)
        temp_hash_bronze.createOrReplaceTempView("tmp_bronze")

        # ---------- Silver: current rows, duplicate check, hash (same method as Bronze) ----------
        df_silver = (spark.table(final_table_name)
                     .filter(F.col("is_latest") == 1)
                     .select(*data_columns))
        if df_silver.groupBy(*ytable_business_key).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"Duplicate business keys found in Silver - {final_table_name}")
        temp_hash_silver = generate_hash_key(df_silver)
        temp_hash_silver.createOrReplaceTempView("tmp_silver")

        # ---------- Compare hashes ----------
        b = spark.table("tmp_bronze").alias("b")
        s = spark.table("tmp_silver").withColumn("_silver_match", F.lit(1)).alias("s")
        join_condition = F.col(f"b.`{ytable_business_key[0]}`") == F.col(f"s.`{ytable_business_key[0]}`")
        for key in ytable_business_key[1:]:
            join_condition = join_condition & (F.col(f"b.`{key}`") == F.col(f"s.`{key}`"))
        comparison = b.join(s, join_condition, "left")

        is_new = F.col("s._silver_match").isNull()
        is_changed = (F.col("s._silver_match").isNotNull()
                      & ~F.col("b.hash_key").eqNullSafe(F.col("s.hash_key")))
        new_records = comparison.filter(is_new).select("b.*")
        changed_records = comparison.filter(is_changed).select("b.*")
        new_count = new_records.count()
        changed_count = changed_records.count()
        print(f"{final_table_name} | New records: {new_count} | Changed records: {changed_count}")

        # ---------- Prepare new versions, rows to expire ----------
        new_versions = (
            new_records.unionByName(changed_records)
            .withColumn("inserted_date", F.current_date())
            .withColumn("updated_date", F.to_date(F.lit(dummy_date)))
            .withColumn("is_latest", F.lit(1))
            .select(*ytable_columns_names)
        )
        expire_records = (changed_records.select(*ytable_business_key)
                          .withColumn("_scd_action", F.lit("EXPIRE")))
        insert_records = new_versions.withColumn("_scd_action", F.lit("INSERT"))
        merge_records = expire_records.unionByName(insert_records, allowMissingColumns=True)

        # ---------- Merge: expire old, insert new ----------
        if new_count + changed_count > 0:
            start_version = get_delta_version(final_table_name)   # version before merge
            merge_condition = " AND ".join(
                [f"t.`{key}` <=> s.`{key}`" for key in ytable_business_key]
                + ["t.`is_latest` = 1", "s.`_scd_action` = 'EXPIRE'"]
            )
            insert_values = {c: f"s.`{c}`" for c in ytable_columns_names}
            (DeltaTable.forName(spark, final_table_name).alias("t")
                .merge(merge_records.alias("s"), merge_condition)
                .whenMatchedUpdate(condition="s.`_scd_action` = 'EXPIRE'",
                                   set={"is_latest": "0", "updated_date": "current_date()"})
                .whenNotMatchedInsert(condition="s.`_scd_action` = 'INSERT'",
                                      values=insert_values)
                .execute())
            # counts from Delta version history
            inserted_count, updated_count = get_merge_counts(final_table_name, start_version)

        # ---------- SUCCESS LOG (also when nothing changed: 0 and 0) ----------
        process_log("Success", table_name=table_name, layer="silver",
                    total_inserted_count=inserted_count,
                    total_updated_count=updated_count)
        print(f"Completed: {final_table_name} | Inserted={inserted_count} | Updated={updated_count}")

    except Exception as e:
        print(f"Failed: {final_table_name} | {type(e).__name__}: {e}")
        failed_tables.append(f"{table_name}: {e}")
        try:
            process_log("Failed", table_name=table_name, layer="silver",
                        total_inserted_count=0, total_updated_count=0,
                        error_message=str(e))
            send_mail_fn(f"{pipeline_name}-{task_name}-{table_name}- Silver layer is failed", str(e))
        except Exception as log_err:
            print(f"Logging/email also failed: {log_err}")

# ---------- Fail the notebook (and the pipeline) after all tables were tried ----------
if failed_tables:
    raise Exception("Silver load failed for: " + " | ".join(failed_tables))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DESCRIBE HISTORY control.etl_run_log")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
