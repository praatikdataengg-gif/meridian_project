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

# Execuiting this notebook to check this file is good and extracting into right folder

# CELL ********************

%run "./common_function"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# pipeline_name = "test_pipeline_1"     
# task_name = "test_task_1"              
# file_name = "client_input_03102026_120202.zip"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#declaring variables
folder_name = datetime.now().strftime("%y%m%d")
run_start_time = datetime.now()
run_id = mssparkutils.env.getJobId()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if not file_name.lower().endswith(".zip"):
    print(f"Skipping {file_name}")
    mssparkutils.notebook.exit("skipped")

final_input_folder = input_folder + file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(pipeline_name, task_name, file_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# file_name = "client_input_03102026_120202_test.zip"
final_input_folder = input_folder+file_name

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#calling unzip function with email notification
validation_message = ""
try:
    process_log("Start",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
    validation_message = unzip_files(final_input_folder, unzip_folder)
    if "Error : " in validation_message:
        raise Exception(validation_message)
    print(f"validation message after execution : {validation_message}")
    time.sleep(15)
    process_log("Success",file_name=file_name)
    spark.sql("""SELECT * FROM control.etl_run_log ORDER BY run_end_time DESC LIMIT 1""").show()
except Exception as e:
    process_log("Failed",file_name=file_name, error_message=f"{validation_message}")
    send_mail_fn(f'{pipeline_name}-{task_name}-{file_name} is failed',f'{validation_message}')
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# print(env, input_folder, unzip_folder, excepted_files)
# import inspect
# print(inspect.signature(unzip_files_v2))
# print("raise Exception" in inspect.getsource(unzip_files_v2))
spark.sql("DELETE FROM control.etl_run_log WHERE run_status = 'Start'")
spark.sql("DELETE FROM control.etl_run_log WHERE run_status = 'Start'").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT file_name, run_status, pipeline_name, run_start_time, error_message
    FROM control.etl_run_log
    ORDER BY run_start_time DESC
""").show(truncate=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
