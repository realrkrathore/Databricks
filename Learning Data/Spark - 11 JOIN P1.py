# Databricks notebook source
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

# MAGIC %md
# MAGIC Create DF From List

# COMMAND ----------

order_df = spark.createDataFrame(orders_list).toDF('order_id','prod_id','unit_price','qty')

# COMMAND ----------

order_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Create Another List For product

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

# MAGIC %md
# MAGIC Creating A DF Using Above Product_list

# COMMAND ----------

product_df =spark.createDataFrame(product_list).toDF("prod_id","prod_name","list_price","qty")
product_df.show()

# COMMAND ----------

order_df.show()
product_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Let Create join Expression

# COMMAND ----------

join_expr = order_df.prod_id == product_df.prod_id

# COMMAND ----------

 order_df.join(product_df,join_expr,'inner').show()


# COMMAND ----------

# MAGIC %md
# MAGIC From Above We Don want all Cols, We Want only 4 cols

# COMMAND ----------

# MAGIC %md
# MAGIC To Avoid Ambigouity , we will rename column name of "qty" in 1 table

# COMMAND ----------

product_renamed_df = product_df.withColumnRenamed("qty","reorder_qty")

# COMMAND ----------

order_df.join(product_renamed_df, join_expr, 'inner')\
 .select("order_id",'prod_name','unit_price','qty')\
 .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Include order_Df Prod_id, but to avoid Ambiguity Error Lets drop prod_id from product_df

# COMMAND ----------

order_df.join(product_renamed_df, join_expr, 'inner')\
    .drop(product_renamed_df.prod_id)\
    .select("order_id",'prod_id','prod_name','unit_price','qty')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Apply Outer Join

# COMMAND ----------

order_df.join(product_renamed_df, join_expr, 'outer')\
    .drop(product_renamed_df.prod_id)\
    .select("*")\
    .sort('order_id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Apply left Outer Join

# COMMAND ----------

order_df.join(product_renamed_df, join_expr, 'left')\
    .drop(product_renamed_df.prod_id)\
    .select("order_id",'prod_id','prod_name','unit_price','list_price','qty')\
    .sort('order_id')\
    .show()

# COMMAND ----------

# MAGIC %md
# MAGIC Lets Replace Null in prod_name  with 'prod_id' and null in list_price with 'unit_price'

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

order_df.join(product_renamed_df, join_expr, 'left')\
    .drop(product_renamed_df.prod_id)\
    .select("order_id",'prod_id','prod_name','unit_price','list_price','qty')\
    .withColumn("prod_name",expr("coalesce(prod_name,prod_id)"))\
    .withColumn("list_price",expr("coalesce(list_price,unit_price)"))\
    .sort('order_id')\
    .show()

# COMMAND ----------


