# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

fire_df = spark.read\
    .format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

fire_df2 = spark.read\
    .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv",
        header = "true",
        inferSchema = "true")

fire_df2.display()    


# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

fire_df.display()

# COMMAND ----------

fire_df.select("Calltype").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Temp View On fire_df

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_service_calls_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from fire_service_calls_view

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a DB

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo_db

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Table in Above The Database

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists demo_db.fire_service_calls_tbl(
# MAGIC   CallNumber integer,
# MAGIC   UnitID string,
# MAGIC   IncidentNumber integer,
# MAGIC   CallType string,
# MAGIC   CallDate string,
# MAGIC   WatchDate string,
# MAGIC   CallFinalDisposition string,
# MAGIC   AvailableDtTm string,
# MAGIC   Address string,
# MAGIC   City string,
# MAGIC   Zipcode integer,
# MAGIC   Battalion string,
# MAGIC   StationArea string,
# MAGIC   Box string,
# MAGIC   OriginalPriority string,
# MAGIC   Priority string,
# MAGIC   FinalPriority integer,
# MAGIC   ALSUnit boolean,
# MAGIC   CallTypeGroup string,
# MAGIC   NumAlarms integer,
# MAGIC   UnitType string,
# MAGIC   UnitSequenceInCallDispatch integer,
# MAGIC   FirePreventionDistrict string,
# MAGIC   SupervisorDistrict string,
# MAGIC   Neighborhood string,
# MAGIC   Location string,
# MAGIC   RowID string,
# MAGIC   Delay float
# MAGIC ) using delta

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo_db.fire_service_calls_tbl 
# MAGIC values(1234, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, 
# MAGIC null, null, null, null, null, null, null, null, null)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC Deleting / Removing Data From The Table

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC INSERING DATA INTO Table From existing temp View

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo_db.fire_service_calls_tbl
# MAGIC select * from fire_service_calls_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC 1> How Many Destinct Type Of Calls were made By Fire Department

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct callType) as Distinct_call_type_count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null

# COMMAND ----------

# MAGIC %md
# MAGIC What were Distict types Of Calls Made To The Fire Department ?
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct callType as distict_call_types
# MAGIC from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %md
# MAGIC find out all responce for delay time greater thn 5 min

# COMMAND ----------

# MAGIC %sql
# MAGIC select callNumber, Delay
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where Delay > 5

# COMMAND ----------

# MAGIC %md
# MAGIC What Were Most Common Call Types

# COMMAND ----------

# MAGIC %sql
# MAGIC Select callType, count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null
# MAGIC group by callType
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC which ZIP Code Accounts from most Common Calls ?

# COMMAND ----------

# MAGIC %sql
# MAGIC Select callType, zipCode , count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null
# MAGIC group by callType, zipCode
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC What ZIP Codes accounted For Most Common Calls ?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT callType, zipCode, count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not Null
# MAGIC group by callType, zipCode
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC What san Francisco neighborhood are in zip Codes 94102 and 94103 ?

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct zipCode, neighborhood
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where zipCode == 94102 or zipCode == 94103
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC What was The SUM of all call alarms , Average, min and max of the call Responce Time ?

# COMMAND ----------

# MAGIC %sql
# MAGIC Select sum(NumAlarms), avg(Delay), min(Delay), max(Delay)
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC How many Distict Years of Data is in Data Set ?

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct year(to_date(callDate,"MM/dd/yyyy")) as year_num
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC order by year_num
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC What is Week Of the year in 2028 had most fire Calls ?

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select weekofyear(to_date(callDate,"MM/dd/yyyy")) week_year, Count(*) as count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate,"MM/dd/yyyy")) == 2018
# MAGIC group by week_year
# MAGIC order by count desc

# COMMAND ----------

# MAGIC %md
# MAGIC What neighborhoods in San Francisco had the Worst response Time in 2018 ?

# COMMAND ----------

# MAGIC %sql
# MAGIC Select neighborhood, delay
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where year(to_date(callDate,"MM/dd/yyyy")) == 2018
# MAGIC order by delay desc

# COMMAND ----------

# MAGIC %md
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Actions And Transformations

# COMMAND ----------

raw_fire_df = spark.read\
    .format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

raw_fire_df.show(10)

# COMMAND ----------

raw_fire_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Above DF Column Name Are not Standarised and Date Feild Are Of String Type

# COMMAND ----------

renamed_fire_df = raw_fire_df.withColumnRenamed("Call Number", "CallNumber")\
    .withColumnRenamed("Unit ID","UnitID")\
    .withColumnRenamed("Incident Number","IncidentNumber")\
    .withColumnRenamed("Call Date","CallDate")\
    .withColumnRenamed("Watch Date","WatchDate")\
    .withColumnRenamed("Call Final Disposition","CallFinalDisposition")\
    .withColumnRenamed("Available DtTm","AvailableDtTm")\
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("ALS Unit", "ALSUnit") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict")

# COMMAND ----------

renamed_fire_df.printSchema()

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now Lets Convert The Data Type of Date Type Column

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

fire_df = renamed_fire_df.withColumn("CallDate",to_date("CallDate","MM/dd/yyyy"))\
  .withColumn("WatchDate",to_date("WatchDate","MM/dd/yyyy"))\
  .withColumn("AvailableDtTm",to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))\
  .withColumn("Delay",round("Delay",2))

# COMMAND ----------

fire_df.display()

# COMMAND ----------

fire_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC Storing Data in Cache

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC Convering DataFrame into Temp View

# COMMAND ----------

fire_df.createOrReplaceTempView("Fire_service_calls_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from Fire_service_calls_view

# COMMAND ----------

# MAGIC %md
# MAGIC How many distinct types of Calls were made to Fire Department ?

# COMMAND ----------

q1 = spark.sql("""
            select count(distinct callType) as Distinct_call_type_count
            from demo_db.fire_service_calls_tbl
            where callType is not null 
            """)

q1.show()

# COMMAND ----------

q1_df = fire_df.where("CallType is not null")\
    .select("CallType")\
    .distinct()

print(q1_df.count())

# COMMAND ----------

q1_df1 = fire_df.where("CallType is not null")
q1_df2 = q1_df1.select("CallType")
q1_df3 = q1_df2.distinct()
print(q1_df3.count())

# COMMAND ----------

q2_df = fire_df.where("CallType is not null")\
    .select("CallType")\
    .distinct()

q2_df.show()

# COMMAND ----------

q2_df2 = fire_df.select("callType as Distinct_call_Type")\
    .where("CallType is not null")\
    .distinct()

q2_df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Find out All Response for Delay time greater than 5 min

# COMMAND ----------

q3_df = fire_df.select("CallNumber",col("Delay"))\
  .where(expr("Delay > 5"))\
  .show()

q3

# COMMAND ----------

# MAGIC %md
# MAGIC What were Most Common Call Types ?

# COMMAND ----------

q4_df = fire_df.select("CallType")\
  .where("callType is not null")\
  .groupBy("CallType")\
  .count()\
  .orderBy("count",ascending = False)\
  .show()
  

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

 orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]

# COMMAND ----------

order_df  = spark.createDataFrame(orders_list).toDF('order_id','prod_id','unit_price','qty')

# COMMAND ----------

order_df.show()

# COMMAND ----------


product_list = [("01", "Scroll Mouse", 250, 20),
                ("02", "Optical Mouse", 350, 20),
                ("03", "Wireless Mouse", 450, 50),
                ("04", "Wireless Keyboard", 580, 50),
                ("05", "Standard Keyboard", 360, 10),
                ("06", "16 GB Flash Storage", 240, 100),
                ("07", "32 GB Flash Storage", 320, 50),
                ("08", "64 GB Flash Storage", 430, 25)]


# COMMAND ----------

product_df = spark.createDataFrame(product_list).toDF("prod_id","prod_name","list_price","qty")
product_df.show()

# COMMAND ----------


