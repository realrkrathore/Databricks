# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Import DF For Bucketing

# COMMAND ----------

df1 = spark.read.json('/Volumes/personal_catalog/default/spark_practice1/ShuffleJoinDemo/data/d1/')
df2 = spark.read.json('/Volumes/personal_catalog/default/spark_practice1/ShuffleJoinDemo/data/d2/')

# COMMAND ----------

df1.show()
df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Data Preperation Need To Be Done using before Bucketing

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS MY_Bucket_DB")
spark.sql("USE MY_BUCKET_DB")

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Bucket Our Dataset and Prepare it For Future Joins (3 Bucket- we ant to run it using 3 threads and want 3 partitions or bucket to acheive max possible parallelism on 3 node cluster and bucket on single "id" key) and Save It as Table in A DB

# COMMAND ----------

df1.coalesce(1).write \
  .bucketBy(3,"id") \
  .mode("overwrite") \
  .saveAsTable("My_Bucket_DB.flight_data1")

# COMMAND ----------

# MAGIC %md
# MAGIC Do Similiar For Other DF

# COMMAND ----------

df2.coalesce(1).write \
  .bucketBy(3,"id") \
  .mode("overwrite") \
  .saveAsTable("My_Bucket_DB.flight_data2")

# COMMAND ----------

# MAGIC %md
# MAGIC now read Both Datasets after bucketing (using table method as they are both table now)

# COMMAND ----------

df3 = spark.read.table("My_bucket_DB.flight_data1")
df4 = spark.read.table("My_bucket_DB.flight_data2")

# COMMAND ----------

# MAGIC %md
# MAGIC To Stop Spark to Auto Apply BroadCast Join

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)

# COMMAND ----------

# MAGIC %md
# MAGIC Now Aplly join On Them

# COMMAND ----------

join_expr = df3.id == df4.id
join_df = df3.join(df4,join_expr,"inner")

# COMMAND ----------


