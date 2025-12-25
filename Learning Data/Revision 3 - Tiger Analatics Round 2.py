# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

df = spark.read.csv("/Volumes/personal_catalog/default/personal_data_volume/sample.csv")
df.show()
df.display()

# COMMAND ----------

orderSchema = "Region String, Country String, UnitSold Integer"

# COMMAND ----------

order_df =  spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",format = "csv",header = True,Schema = orderSchema)

# COMMAND ----------

order_df.show()

# COMMAND ----------

order_df.printSchema()

# COMMAND ----------

colList= df.columns
print(colList)

# COMMAND ----------

order_df.show()

# COMMAND ----------

order_df.select("Region","Country").show()

# COMMAND ----------

display(order_df.select(max("UnitSold").alias("MaxUnit")))

# COMMAND ----------

display(order_df.select("UnitSold"))

# COMMAND ----------

order_df.createOrReplaceTempView("dfVw")

# COMMAND ----------

spark.sql("Select *  from dfVw").show()

# COMMAND ----------

display(spark.sql("Select max(UnitSold) from dfVw"))

# COMMAND ----------

row1 = Row("Japan-East","Japan",9)

# COMMAND ----------

rowDf = spark.createDataFrame([row1],orderSchema)
rowDf.show()

# COMMAND ----------

tempRow1 = Row("Japan-South","Japan",9)
tempDf= spark.createDataFrame([tempRow1],orderSchema)
tempDf.show()

# COMMAND ----------

order_df.select(expr("Country as CountryName")).show()

# COMMAND ----------

newOrderDf = order_df.withColumn("Foreign",lit(1))
newOrderDf.show()

# COMMAND ----------

newOrderDf2 = order_df.withColumn("Foreign",expr("Country != 'India'"))
newOrderDf2.show()

# COMMAND ----------

newOrderDf2 = order_df.withColumn("Foreign",expr("int(Country != 'India')"))
newOrderDf2.show()

# COMMAND ----------

newOrderDf3 = newOrderDf2.withColumnRenamed("Foreign","ForeignCountry")
newOrderDf3.show()

# COMMAND ----------

newOrderDf4 = newOrderDf3.drop("ForeignCountry")
newOrderDf4.show()

# COMMAND ----------

newOrderDf3.filter("UnitSold > 4").show()

# COMMAND ----------

newOrderDf3.filter(col("UnitSold")>4).show()

# COMMAND ----------

spark.sql("Select * from dfVw").show()

# COMMAND ----------

spark.sql("Select * from dfVw where unitSold > 4").show()

# COMMAND ----------

new_row = [Row("India-South","India",2)]
newOrderDf5 = newOrderDf4.union(spark.createDataFrame(new_row))
newOrderDf5.show()

# COMMAND ----------

newOrderDf5.select("Country","Region").distinct().show()

# COMMAND ----------

display(newOrderDf5.dropDuplicates(["Country"]))

# COMMAND ----------

orderDf6 = newOrderDf4.union(newOrderDf5)
orderDf6.show()

# COMMAND ----------

orderDf6.sort("Country","Region").show()

# COMMAND ----------

orderDf6.sort(expr("Country DESC")).show()

# COMMAND ----------

display(orderDf6.orderBy(col("UnitSold").asc()))
display(orderDf6.orderBy(col("UnitSold").desc()))

# COMMAND ----------

orderDf7=orderDf6.limit(5)
orderDf7.show()

# COMMAND ----------

orderDf7.select(initcap("Region")).show()

# COMMAND ----------

orderDf7.select(upper("Region")).show()

# COMMAND ----------

orderDf7.select(concat("Region","Country")).show()

# COMMAND ----------

orderDf7.select(concat_ws("#","Region","Country")).show()

# COMMAND ----------

tDf1 = orderDf7.withColumn("Today",current_date())
tDf1.show()

# COMMAND ----------

tDf2 = tDf1.withColumn("TodayTimeStamp",current_timestamp())
tDf2.show()


# COMMAND ----------

tDf2.select(datediff("Today","Today")).show()

# COMMAND ----------

tDf2.select(year("Today")).show()

# COMMAND ----------

tDf2.select(month("Today")).show()

# COMMAND ----------

tDf2.select(dayofmonth("Today")).show()

# COMMAND ----------

mySchema = "Date String,Country String"
myRow = Row("13-09-2025","India")

# COMMAND ----------

myDf = spark.createDataFrame([myRow],mySchema)
myDf.show()
myDf.printSchema()

# COMMAND ----------

myDateFormat = "dd-mm-yyyy"

# COMMAND ----------

cleanDf = myDf.select(to_date("Date",myDateFormat).alias("DateInDateFormat"))
cleanDf.show()
cleanDf.printSchema()

# COMMAND ----------

tDf1.write.mode("csv").mode("overwrite").save('/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/')

# COMMAND ----------

# MAGIC %md
# MAGIC ------

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

fire_df = spark.read\
    .format("csv")\
    .option("header","true")\
    .option("inferSchema","true")\
    .load("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

fire_df.show()

# COMMAND ----------

fire_df2 = spark.read\
    .csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv",header="true",inferSchema="true")

fire_df2.show()

# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

fire_df.display()

# COMMAND ----------

fire_df.select("CallType").show()

# COMMAND ----------

fire_df.createOrReplaceTempView('fire_dfVw')

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from fire_dfVw

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo_db

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
# MAGIC Select * from demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE demo_db.fire_service_calls_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into demo_db.fire_service_calls_tbl
# MAGIC select * from fire_dfVw
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo_db.fire_service_calls_tbl

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

renamed_fire_df.printSchema()

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *

# COMMAND ----------

fire_df = renamed_fire_df.withColumn("CallDate",to_date("CallDate",'MM/dd/yyyy'))\
    .withColumn("WatchDate",to_date("WatchDate","MM/dd/yyyy"))\
    .withColumn("AvailableDtTm",to_timestamp("AvailableDtTm","MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("Delay",round("Delay",2))

# COMMAND ----------

fire_df.display()

# COMMAND ----------

fire_df.cache()

# COMMAND ----------

q1_df = fire_df.select("callType").distinct()
q1_df.show()
q1_df.count()

# COMMAND ----------

q3_df = fire_df.select("CallNumber","Delay")\
    .where(expr("Delay > 5"))

q3_df.show()

# COMMAND ----------

q4_df = fire_df.select("callType")\
    .groupBy("CallType")\
    .count()\
    .orderBy("Count",ascending = False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC -----

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

order_df = spark.createDataFrame(orders_list).toDF('order_id','prod_id','unit_price','qty')

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

prodSchema = "prod_id String, prod_name String, list_price integer, qty integer"

# COMMAND ----------

prod_df = spark.createDataFrame(product_list,prodSchema)
prod_df.show()

# COMMAND ----------


