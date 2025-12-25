# Databricks notebook source
fire_df = spark.read\
    .format("csv")\
    .option("inferSchema","true")\
    .option("header","true")\
    .load('/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv')

# COMMAND ----------

df = spark.read.option("inferSchema","true").option("header","true").csv("/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

fire_df.show(10)

# COMMAND ----------

fire_df.display()

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

df.createOrReplaceTempView("df_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from df_view;

# COMMAND ----------

df = spark.read.csv("/Volumes/personal_catalog/default/personal_data_volume/sample.csv")
df.show()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

orderSchema = StructType([StructField("Region",StringType(),True),
                          StructField("Country",StringType(),True),
                          StructField("UnitSold",IntegerType(), True)])

# COMMAND ----------

order_df = spark.read.load("/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv",format = "csv",header=True,Schema=orderSchema)
order_df.show()

# COMMAND ----------

order_df2 = spark.read\
    .format("csv")\
    .option("header","true")\
    .option("schema","orderSchema")\
    .option("path","/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv")\
    .load()

order_df2.show()

# COMMAND ----------

orderSchema2 = "Region String, Country String, UnitSold Integer"

# COMMAND ----------

orderdf2 = spark.read\
    .format("csv")\
    .option("header","true")\
    .option("schema","orderSchema2")\
    .option("path","/Volumes/personal_catalog/default/spark_practice1/CountrySample.csv")\
    .load()

# COMMAND ----------

orderdf2.show()

# COMMAND ----------

orderdf2.printSchema()

# COMMAND ----------

orderdf2.describe().show()

# COMMAND ----------

colList = orderdf2.columns

# COMMAND ----------

colList

# COMMAND ----------

df = orderdf2.select("Country","UnitSold")
df.show()

# COMMAND ----------

df = orderdf2.select("Country",col("UnitSold"),column("Region"))
df.show()

# COMMAND ----------

display(df.select(max("UnitSold")))

# COMMAND ----------

display(df.select(max("UnitSold").alias("MaxUnit")))

# COMMAND ----------

df.createOrReplaceTempView("dfView")

# COMMAND ----------

display(spark.sql("Select * from dfView"))

# COMMAND ----------

row1 = Row("Japan-East","Japan",9)

# COMMAND ----------

df.select(expr("Country as CountryName")).show()

# COMMAND ----------

newDF = df.withColumn("Foreign",lit(1))
newDF.show()

# COMMAND ----------

newdf2 = df.withColumn("Foreign",expr("Country != 'India' "))
newdf2.show()

# COMMAND ----------

newdf3 = newdf2.withColumnRenamed("Foreign","ForeignCountry")
newdf3.show()

# COMMAND ----------

newdf4 = newdf3.drop("ForeignCountry")
newdf4.show()

# COMMAND ----------


