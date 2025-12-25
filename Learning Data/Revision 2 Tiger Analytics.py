# Databricks notebook source
df = spark.read.csv("/Volumes/personal_catalog/default/personal_data_volume/sample.csv")
df.show()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

orderSchema = StructType([StructField("Region",StringType(),True),
                          StructField("Country",StringType(),True),
                          StructField("UnitSold",IntegerType(),True)])

# COMMAND ----------

order_df = spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",format="csv",header=True,Schema = orderSchema)

# COMMAND ----------

order_df.show()

# COMMAND ----------

orderSchema2 = "Region String, Country String, UnitSold Integer"

# COMMAND ----------

order_df2 = spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",header= True, format = "csv", schema = orderSchema2)
order_df2.show()

# COMMAND ----------

order_df.printSchema()

# COMMAND ----------

colList = df.columns
display(colList)

# COMMAND ----------

from pyspark.sql.functions import column, col, max

# COMMAND ----------

from pyspark.sql.functions import col

order_df.select(col("Region"),column("Country")).show()

# COMMAND ----------

display(order_df.select(column("Region"),col("UnitSold"),order_df.Country))

# COMMAND ----------

display(order_df.select(max(col("UnitSold"))))

# COMMAND ----------

display(order_df.select("UnitSold"))

# COMMAND ----------

display(order_df.select(max("UnitSold").alias("MaxUnit")))

# COMMAND ----------

order_df.createOrReplaceTempView("order_df_tempViewTable")

# COMMAND ----------

spark.sql("Select max(UnitSold) From order_df_tempViewTable").show()

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

row1 = Row("Japan-East","Japan",9)

# COMMAND ----------

rowDf = spark.createDataFrame([row1],orderSchema)

# COMMAND ----------

rowDf.show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

order_df.select(expr("Country as CountryName")).show()

# COMMAND ----------

order_df.select(col("Country").alias("CountryName")).show()

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

newOrderDf = order_df.withColumn("Foreign",lit(1))
newOrderDf.show()

# COMMAND ----------

newOrderDf2 = order_df.withColumn('Foreign',expr("Country !='India'"))
newOrderDf2.show()

# COMMAND ----------

newOrderDf3 = order_df.withColumn("Foreign",expr("int( Country != 'India')"))
display(newOrderDf3)

# COMMAND ----------

newOrderDf4 = newOrderDf3.withColumnRenamed("Foreign","ForeignCountry")
newOrderDf4.show()

# COMMAND ----------

newOrderDf5 = newOrderDf4.drop("ForeignCountry")
newOrderDf5.show()

# COMMAND ----------

newOrderDf3.filter("UnitSold > 4").show()

# COMMAND ----------

newOrderDf3.filter(col("UnitSold")> 4).show()

# COMMAND ----------

spark.sql("Select * from order_df_tempViewTable Where UnitSold > 4").show()

# COMMAND ----------

newRow = [Row("India-South","India",2)]
newOrderDf7 = newOrderDf5.union(spark.createDataFrame(newRow))
newOrderDf7.show()

# COMMAND ----------

newOrderDf7.select("Country","Region").distinct().show()

# COMMAND ----------

display(newOrderDf7.dropDuplicates(["Country"]))

# COMMAND ----------

orderDf8 = newOrderDf5.union(newOrderDf7)
newOrderDf5.show()
newOrderDf7.show()
orderDf8.show()

# COMMAND ----------

orderDf8.sort("UnitSold").show()

# COMMAND ----------

orderDf8.orderBy("UnitSold").show()

# COMMAND ----------

orderDf8.sort("Country","Region").show()

# COMMAND ----------

orderDf8.sort(expr("Country Desc")).show()

# COMMAND ----------

display(orderDf8.orderBy(col("UnitSold").asc()))
display(orderDf8.orderBy(col("UnitSold").desc()))

# COMMAND ----------

orderDf8.show()

# COMMAND ----------

orderDf9 = orderDf8.limit(5)
orderDf9.show()

# COMMAND ----------

# MAGIC %md
# MAGIC String

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

orderDf9.select(initcap("Region")).show()

# COMMAND ----------

orderDf9.select(upper("Region")).show()

# COMMAND ----------

orderDf9.select(lower("Region")).show()

# COMMAND ----------

orderDf9.select(concat("Region","Country")).show()

# COMMAND ----------

orderDf9.select(concat_ws("#","Region","Country")).show()

# COMMAND ----------

orderDf9.select(concat_ws("#","UnitSold","Country")).show()

# COMMAND ----------

orderDf9.select(instr("Country","India")).show()

# COMMAND ----------

orderDf9.select(length("Country").alias("Country_Length")).show()

# COMMAND ----------

tdf1 =orderDf9.withColumn("Today",current_date())
tdf1.show()

# COMMAND ----------

tdf2 = tdf1.withColumn("TodayTimeStamp",current_timestamp())
tdf2.show()

# COMMAND ----------

tdf2.select(datediff("Today","Today")).show()

# COMMAND ----------

tdf2.select(datediff("Today","TodayTimeStamp")).show()

# COMMAND ----------

tdf2.select(year("Today")).show()

# COMMAND ----------

tdf2.select(year("TodayTimeStamp")).show()

# COMMAND ----------

tdf2.select(month("Today")).show()

# COMMAND ----------

tdf2.select(dayofmonth("Today")).show()

# COMMAND ----------

tdf2.select(expr("hour(to_timestamp(TodayTimeStamp))")).show()

# COMMAND ----------

mySchema = "Date String, Country String"

# COMMAND ----------

myRow = Row("13-09-2025","India")

# COMMAND ----------

myDf = spark.createDataFrame([myRow],mySchema)
myDf.show()
myDf.printSchema()

# COMMAND ----------

myDateformat = "dd-MM-yyyy"

# COMMAND ----------

cleanDf = myDf.select(to_date("Date",myDateformat)).alias("DateinDateFromat")
cleanDf.show()
cleanDf.printSchema()

# COMMAND ----------

df.na.drop().display()

# COMMAND ----------

df.na.drop('all').display()

# COMMAND ----------

df.na.fill("SomeValue").count()

# COMMAND ----------

tdf1.show()

# COMMAND ----------

tdf1.write.format("csv").mode("overwrite").save('/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/')

# COMMAND ----------

tdf1.write.format("csv").mode("overwrite").option('path','/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/').save()

# COMMAND ----------

tdf1.write.saveAsTable("TAprac1table")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from TAprac1table

# COMMAND ----------

tdf1.write.option('path','/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/').saveAsTable("TAprac2table")

# COMMAND ----------

# MAGIC %md
# MAGIC -------

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

fire_df = spark.read\
  .format("csv")\
  .option("header","true")\
  .option("inferSchema","true")\
  .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

fire_df.show()

# COMMAND ----------

fireDf2 = spark.read\
    .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv",header = "true",inferSchema = "true")

fireDf2.show()

# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

fire_df.display()

# COMMAND ----------

fire_df.select("CallType").display()

# COMMAND ----------

fire_df.createOrReplaceTempView("fire_services_calls_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire_services_calls_view

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists TA_demo_db

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
# MAGIC select * from TA_demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table demo_db.fire_Service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.fire_Service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo_db.fire_service_calls_tbl
# MAGIC select * from fire_services_calls_view

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(distinct callType) as Distinct_calltype_Count
# MAGIC from  demo_db.fire_service_calls_tbl
# MAGIC where callType is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC Select distinct callType as distict_call_types
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where callType is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select callNumber,Delay
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC where Delay > 5

# COMMAND ----------

# MAGIC %sql
# MAGIC Select callType,count(*) AS Count
# MAGIC from demo_db.fire_service_calls_tbl
# MAGIC Where callType is not null
# MAGIC group by callType 
# MAGIC order by count desc

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

renamed_fire_df = raw_fire_df.withColumnRenamed("Call Number","CallNumber")\
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

display(renamed_fire_df)

# COMMAND ----------

fire_df = renamed_fire_df.withColumn("CallDate",to_date("CallDate","MM/dd/yyyy"))\
    .withColumn("WatchDate",to_date("WatchDate","MM/dd/yyyy"))\
  .withColumn("AvailableDtTm",to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))\
  .withColumn("Delay",round("Delay",2))

# COMMAND ----------

q1 = spark.sql("""Select count(distinct callType) AS                 Distinct_call_type_Count
               from demo_db.fire_service_calls_tbl
            where callType is not null """)

q1.show()

# COMMAND ----------

q1_df =fire_df.where("CallType is not null")\
    .select("CallType")\
    .distinct()

print(q1_df.count())

# COMMAND ----------

q1_df = fire_df.select("CallType")\
    .where("CallType is not null")\
    .distinct()

q1_df.show()

# COMMAND ----------

q3_df = fire_df.select("CallNumber","Delay")\
    .where(expr("Delay > 5"))\
        .show()

# COMMAND ----------

q4_df = fire_df.select("CallType")\
    .where("callType is not null")\
    .groupBy("CallType")\
    .count()\
    .orderBy("count",ascending = False)

q4_df.show()


# COMMAND ----------


