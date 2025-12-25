# Databricks notebook source
# MAGIC %md
# MAGIC Lets Create A Spark Session with 3 parallel Threads

# COMMAND ----------

from pyspark.sql import *


# COMMAND ----------

# spark = SparkSession\
#   .builder\
#   .appName("Suffle Join Demo")\
#   .master("local[3]")\
#   .getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC lets Read Data from d1 and d2 folders in 2 df

# COMMAND ----------

flight_time_df1 = spark.read.json('/Volumes/personal_catalog/default/spark_practice1/ShuffleJoinDemo/data/d1/')
flight_time_df2 = spark.read.json('/Volumes/personal_catalog/default/spark_practice1/ShuffleJoinDemo/data/d2/')

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Set Shuffle Configuration, This Cofig will make Sure that we have 3 partition after the shuffle (means having 3 reduce Exhanges)

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions",3)

# COMMAND ----------

# MAGIC %md
# MAGIC Now Lets Define the Join Expression

# COMMAND ----------

join_expr = flight_time_df1.id == flight_time_df2.id

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Perform Join Operation

# COMMAND ----------

join_df = flight_time_df1.join(flight_time_df2,join_expr,'inner')

# COMMAND ----------

display(join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Now Lets Apply BraodCast Join On above DFs (lets Apply broadcast Function on right Side DF)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

join_df2 = flight_time_df1.join(broadcast(flight_time_df2),join_expr,'inner')

# COMMAND ----------

display(join_df2)

# COMMAND ----------


