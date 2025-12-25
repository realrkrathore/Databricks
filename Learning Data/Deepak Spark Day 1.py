# Databricks notebook source
# MAGIC %md
# MAGIC Reading Spark 

# COMMAND ----------

df = spark.read.csv("/Volumes/personal_catalog/default/personal_data_volume/sample.csv")
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Defining Schema Programmatically for Dataframes

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

OrderSchema = StructType([StructField("Region" , StringType(), True),
                          StructField("Country" , StringType(),True),
                          StructField("UnitSold", IntegerType(),True)])

# COMMAND ----------

# MAGIC %md
# MAGIC Loading Data Using The Schema

# COMMAND ----------

order_df = spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",format="csv",header = True, Schema = OrderSchema)
order_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Defining Dataframe Schema Using SQL

# COMMAND ----------

OrderSchema2 = "Region String, Country String, UnitSold Integer"

# COMMAND ----------

order_df2 = spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",header = True,format = "csv" ,  schema = OrderSchema2)
order_df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Print Schema

# COMMAND ----------

order_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC To See List Of Columns

# COMMAND ----------

colList = df.columns


# COMMAND ----------

# MAGIC %md
# MAGIC Accessing Column Using Column Name

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

display(order_df.select(max(col("UnitSold"))))

# COMMAND ----------

display(order_df.select("UnitSold"))

# COMMAND ----------

display(order_df.select(max("UnitSold").alias("MaxUnit")))

# COMMAND ----------

# MAGIC %md
# MAGIC Creating A Temp View From DF

# COMMAND ----------

order_df.createOrReplaceTempView("order_df_tempViewTable")

# COMMAND ----------

# MAGIC %md
# MAGIC RUN SQL Query Over TempView

# COMMAND ----------

display(spark.sql("Select max(UnitSold) From order_df_tempViewTable"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a Data Set And Inserting It In The DF and Using Manually Created Schema

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

row1 = Row("Japan-East","Japan",9)

# COMMAND ----------

# MAGIC %md
# MAGIC Creating DF Using Above row1 Dataset

# COMMAND ----------

rowDF= spark.createDataFrame([row1],OrderSchema)

# COMMAND ----------

rowDF.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Selecting Max Of Country and Using EXPR

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

order_df.select(expr("Country as CountryName")).show()

# COMMAND ----------

order_df.select(col("Country").alias("CountryName")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding A new Column with default Value as 1

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

newOrderDf = order_df.withColumn("Foreign",lit(1))
newOrderDf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding A new Columns but inserting Value On Behalf Of Conditions

# COMMAND ----------

newOrderDf2 = order_df.withColumn("Foreign", expr("Country != 'India'"))
display(newOrderDf2)

# COMMAND ----------

newOrderDf2 = order_df.withColumn("Foreign", expr("int(Country != 'India')"))
display(newOrderDf2)

# COMMAND ----------

# MAGIC %md
# MAGIC Edit Column Name

# COMMAND ----------

newOrderDf3 = newOrderDf2.withColumnRenamed("Foreign","ForeignCountry")
newOrderDf3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping Column

# COMMAND ----------

newOrderDf4 = newOrderDf3.drop("ForeignCountry")
newOrderDf4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering Row

# COMMAND ----------

newOrderDf3.filter("UnitSold > 4").show()

# COMMAND ----------

newOrderDf3.filter(col("UnitSold") > 4).show()

# COMMAND ----------

spark.sql("Select * from order_df_tempViewTable").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Using SQL To Filter

# COMMAND ----------

spark.sql("Select * from order_df_tempViewTable where UnitSold > 4").show()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding New Row To DF

# COMMAND ----------

from pyspark.sql import Row

# COMMAND ----------

new_row = [Row("India-South","India",2)]
newOrderDf5 = newOrderDf4.union(spark.createDataFrame(new_row))
newOrderDf5.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Filtering Distinct Data

# COMMAND ----------

newOrderDf5.select("Country","Region").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC Show Everything about the Data Where Country is Distict

# COMMAND ----------

display(newOrderDf5.dropDuplicates(["Country"]))

# COMMAND ----------

# MAGIC %md
# MAGIC Combining 2 DF - Union

# COMMAND ----------

OrderDf6 = newOrderDf4.union(newOrderDf5)
OrderDf6.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting

# COMMAND ----------

OrderDf6.sort("UnitSold").show()

# COMMAND ----------

OrderDf6.orderBy("UnitSold").show()

# COMMAND ----------

OrderDf6.sort("Country","Region").show()

# COMMAND ----------

OrderDf6.sort(expr("Country Desc")).show()

# COMMAND ----------

from pyspark.sql.functions import col

display(OrderDf6.orderBy(col("UnitSold").asc()))
display(OrderDf6.orderBy(col("UnitSold").desc()))

# COMMAND ----------

OrderDf6.show()

# COMMAND ----------

OrderDf7 = OrderDf6.limit(5)
OrderDf7.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC STRING Functions

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

OrderDf7.select(initcap("Region")).show()

# COMMAND ----------

OrderDf7.select(upper("Region")).show()

# COMMAND ----------

OrderDf7.select(lower(col("Region"))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Concating Two Columns

# COMMAND ----------

OrderDf7.select(concat("Region","Country")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Concating With a Seprator

# COMMAND ----------

OrderDf7.select(concat_ws('#',"Region","Country")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Whether String Accours in Column (Boolean O/P)

# COMMAND ----------

OrderDf7.select(instr("Country","India")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Give Length Of Character in Each Column

# COMMAND ----------

OrderDf7.select(length("Country").alias("CountryLength")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Data Handling In Spark

# COMMAND ----------

# MAGIC %md
# MAGIC Adding Todays Date In A New Column

# COMMAND ----------

tDf1 = OrderDf7.withColumn("Today",current_date())
tDf1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Adding New Column With Current TimeStamp

# COMMAND ----------

tDf2 = tDf1.withColumn("TodayTimeStamp",current_timestamp())
tDf2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Difference Between Two Date Column

# COMMAND ----------

tDf2.select(datediff("Today","Today")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Comparing The Difference Between Data And Time Stamp Column

# COMMAND ----------

tDf2.select(datediff("Today","TodayTimeStamp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Month Between

# COMMAND ----------

tDf2.select(months_between("Today","TodayTimeStamp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Bring Out Month Ot Of Date Column

# COMMAND ----------

tDf2.select(year("Today")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Bring Out Month Ot Of DateTime Column

# COMMAND ----------

tDf2.select(year("TodayTimeStamp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Bring Out Month out Of Date type Column

# COMMAND ----------



# COMMAND ----------

tDf2.select(month("TodayTimeStamp")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC Bring Out Date out Of Date type Column

# COMMAND ----------

tDf2.select(dayofmonth("TodayTimeStamp")).show()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Bring Out Hours Out Of TimeStamp type Column

# COMMAND ----------

tDf2.select(expr("hour(to_timestamp(TodayTimeStamp))")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Converting A String To Date Type Format

# COMMAND ----------

mySchema = "Date String,Country String"

# COMMAND ----------


myRow = Row("13-09-2025","India")

# COMMAND ----------

myDf = spark.createDataFrame([myRow],mySchema)
myDf.show()
myDf.printSchema()

# COMMAND ----------

myDateformat = 'dd-MM-yyyy'

# COMMAND ----------

cleanDf = myDf.select(to_date("Date",myDateformat).alias("DateInDateFormat"))
cleanDf.show()
cleanDf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC **Handle Null Values**

# COMMAND ----------

# MAGIC %md
# MAGIC Test Later

# COMMAND ----------

df.na.drop().display()

# COMMAND ----------

df.na.drop('all').display()

# COMMAND ----------

df.na.drop("any").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop When Specific Column Value is Null

# COMMAND ----------

df.na.drop("all",subset=["_c5","_c3"]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Fill Null Values With SomeValue

# COMMAND ----------

df.na.fill("SomeValue").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Data

# COMMAND ----------

# MAGIC %md
# MAGIC Saving DF as CSV

# COMMAND ----------

tDf1.show()

# COMMAND ----------

tDf1.write.format("csv").mode('overwrite').save('/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/')

# COMMAND ----------

# MAGIC %md
# MAGIC Or 

# COMMAND ----------

tDf1.write.format("csv").mode("overwrite").option('path','/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/').save()

# COMMAND ----------

# MAGIC %md
# MAGIC Saving DF as table

# COMMAND ----------

tDf1.write.saveAsTable("tDf1asTable")

# COMMAND ----------

# MAGIC %sql
# MAGIC  DESCRIBE DETAIL workspace.default.tdf1astable;

# COMMAND ----------

# MAGIC %fs ls /user/hive/warehouse/

# COMMAND ----------

# MAGIC %md
# MAGIC Creating a DB

# COMMAND ----------

spark.sql("Create database DeepakDb")

# COMMAND ----------

tDf1.write.saveAsTable("DeepakDb.RajatTable")

# COMMAND ----------

# MAGIC %md
# MAGIC Saving Table As External Table

# COMMAND ----------

tDf1.write.option("path"'/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/').saveAsTable("DeepakDb.RajatTable2")

# COMMAND ----------

tDf1.write.option("path", "s3a://personal_catalog/default/spark_practice1/OutputWriteQuery/").saveAsTable("DeepakDb.RajatTable2")

# COMMAND ----------

# MAGIC %md
# MAGIC External Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC Create external table XYZ(Regions String,Country String) location '/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/';

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mounting

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting Using SAS Token

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Mounting Using APP REGISTERATION

# COMMAND ----------

# MAGIC %md
# MAGIC See Folder and Files at mounted Location

# COMMAND ----------

ls /mnt/RajatData
