# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b859230d-c2b7-4fde-9f79-0ef714fd9c7a",
# META       "default_lakehouse_name": "lk_vdp",
# META       "default_lakehouse_workspace_id": "c4eb058f-0e68-4d26-84de-d6d954df390b",
# META       "known_lakehouses": [
# META         {
# META           "id": "b859230d-c2b7-4fde-9f79-0ef714fd9c7a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

#library
from pyspark.sql.functions import col
from pyspark.sql import functions as F
# Welcome to your new notebook
# Type here in the cell editor to add code!
#create dataframe
emp_data = [
    (1, "Amit", "Pune", "2026-09-26 11:12:20"),
    (2, "Pratik", "Mumbai", "2026-09-26 11:12:21"),
    (3, "Nikhil", "Satara", "2026-09-26 11:12:22"),
    (3, "Nikhil", "Satara", "2026-09-26 11:12:23"),
    (3, "Nikhil", "Delhi", "2026-09-26 11:12:24")
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#create data frame from above data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp = spark.createDataFrame(emp_data, ["id", "names", "city", "created_date"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_emp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# display(df_emp.select("names", "city"))                                                     
# df_filter = df_emp.filter(df_emp.city ==  "Satara")
# display(df_filter)
# df_filter = df_emp.filter(F.col("city") ==  "Satara")
# display(df_filter)
# df_filter = df_emp.filter(F.col("city") .isin("Satara","Pune"))
# display(df_filter)
# df_filter = df_emp.filter(
#     (F.col("city") .isin("Satara","Pune")) &
#     (F.col("id") == 3)
#     )
# display(df_filter)

# df_filter = df_emp.filter(
#     (~F.col("city") .isin("Satara","Pune")) 
#     )
# display(df_filter)
# df_filter = df_emp.filter(
#     (F.col("city").like("Pune%")) 
#     )
# display(df_filter)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# add new column in exsiting frame
df_emp = df_emp.withColumn("country", F.lit("India"))
df_emp = df_emp.withColumnRenamed("country", "countries")
df_emp = df_emp.withColumnRenamed("created_datecity", "city")
df_emp = df_emp.withColumnRenamed("_4", "created_date")
df_emp = df_emp.orderBy(F.desc("id"), F.asc("names"))
display(df_emp)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dataframes display with alias 
display(df_emp.select(F.col("id").alias("serialNumber")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#find count of dataframe
df_emp.count()
selectedColumnCount = df_emp.select(F.col("id")).distinct().count()
print("selectedColumnCount => ", selectedColumnCount)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicatesRemove = df_emp.dropDuplicates()
display(duplicatesRemove)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#how to remove partial duplicates using ranking function
from pyspark.sql.window import Window
window_spect = Window.partitionBy("id", "names").orderBy(F.desc("created_date"))
df_rank = duplicatesRemove.withColumn("rn", F.row_number().over(window_spect)).filter(F.col("rn") == "1")
display(df_rank)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = [
    ("Engineering", 1, 90000),
    ("Engineering", 2, 85000),
    ("Sales", 3, 60000),
    ("Sales", 4, 65000),
    ("HR", 5, 55000),
]
columns = ["department", "employee_id", "salary"]

df = spark.createDataFrame(data, columns)
df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.groupBy("department").agg(F.count("*").alias("employee_id"),
F.sum("salary").alias("total_salary")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

salary_df.agg(F.count("*").alias("employee_count"),
F.sum("salary").alias("total_salary")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp = [
    (1,"Ganesh"),
    (2,"Pratik"),
    (3,"Vipul")
]

department = [
    (1,"IT"),
    (2,"HR"),
    (4,"ADMIN")
]

df_emp = spark.createDataFrame(emp,["id","name"])

df_department = spark.createDataFrame(department,["id","department"])

display(df_emp)
display(df_department)
#df_emp.join(df_department,"id","inner").show()
# df_emp.join(df_department,"id","left").show()
# df_emp.join(df_department,"id","right").show()
# df_emp.join(df_department,"id","outer").show()
# df_emp.join(df_department,"id","left_anti").show()
# df_department.join(df_emp,"id","anti").show()
# df_department.join(df_emp,"id","semi").show()
df_department.join(df_emp,how="cross").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# joins with multiplecolumn
emp = [
    (1,"Ganesh",101),
    (2,"Pratik",101),
    (3,"Vipul",104)
]

department = [
    (1,"IT",101),
    (2,"HR",102),
    (4,"ADMIN",104)
]

df_emp = spark.createDataFrame(emp,["emp_id","name", "emp_dept_id"])

df_department = spark.createDataFrame(department,["employee_id","department", "emp_department"])

display(df_emp)
display(df_department)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp.join(df_department,( df_emp.emp_id == df_department.employee_id)
&
(df_emp.emp_dept_id==df_department.emp_department), "inner").select("emp_id", "name", "department").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%fs ls Files/Emp


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df = spark.read.format("csv")\
.option("delimiter","|")\
.option("header", True)\
.option("inferSchema", True)\
.load("Files/Emp")
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#design custom schema
from pyspark.sql.types import StructField, IntegerType, StringType, StructType
custom_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("emp_name", StringType(), True),
    StructField("emp_city", StringType(), True)
])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# read all files one by one from Emp folder
# make  a list of all files in one array 
all_files = mssparkutils.fs.ls("Files/Emp") 
# print(all_files)
for f in all_files:
    # print(f.path)
    emp_df = spark.read.format("csv")\
    .option("delimiter","|")\
    .option("header", True)\
    .option("inferSchema", True)\
    .load(f.path)

    emp_df.write.mode("append").saveAsTable('emp_data')
display(emp_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- TRUNCATE table emp_data 
# MAGIC select * from emp_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.table("emp_data").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
