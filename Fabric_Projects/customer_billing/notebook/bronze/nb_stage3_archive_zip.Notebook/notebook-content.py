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
run_id = mssparkutils.env.getJobId() + f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

archive_folder = "Files/ziparchive/" + datetime.now().strftime("%Y%m%d")
source_zip_path = f"Files/inbound/customer_billing/{file_name}"
archive_zip_path = os.path.join(archive_folder, os.path.basename(file_name))

#Log process started
process_log("Start", table_name="ZIP_ARCHIVAL", layer="bronze", error_message=None)

try:
    #Create dated archive folder
    mssparkutils.fs.mkdirs(archive_folder)

    #Archive ZIP
    if not mssparkutils.fs.exists(source_zip_path):
        raise FileNotFoundError(f"Original ZIP not found: {source_zip_path}")

    if mssparkutils.fs.exists(archive_zip_path):
        raise FileExistsError(f"Archive destination already exists: {archive_zip_path}")

    mssparkutils.fs.mv(source_zip_path, archive_zip_path)
    print(f"ZIP archived successfully: {archive_zip_path}")

    #Delete extracted folder after successful archival
    unzip_folder = "Files/unzip/" + datetime.now().strftime("%Y%m%d")
    if mssparkutils.fs.exists(unzip_folder):
        mssparkutils.fs.rm(unzip_folder, True)
        print(f"Extracted folder deleted: {unzip_folder}")
    else:
        print(f"Extracted folder not found: {unzip_folder}")

    #Log success
    process_log("Success", table_name="ZIP_ARCHIVAL", layer="bronze", error_message=None)

except Exception as e:
    #Log failure
    process_log("Failed", table_name="ZIP_ARCHIVAL", layer="bronze", error_message=str(e))
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
