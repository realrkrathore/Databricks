# Databricks notebook source
# MAGIC %md
# MAGIC Fibonacii Series Print

# COMMAND ----------

def fib(n):
  a,b=0,1
  if n <= 0:
    return -1
  elif n == 1:
    return a 
  else:
    print(a)
    for i in range(1,n):
      print(b)
      temp = a+b
      a=b
      b=temp


# COMMAND ----------

# MAGIC %md
# MAGIC Print nth Term of Fibonacci Series

# COMMAND ----------

def fib(n):
    a,b= 0,1
    if n <= 0:
        return -1
    elif n == 1:
        return a
    else :
        for i in range(1,n):
            temp = a + b
            a = b
            b = temp

        return a



# COMMAND ----------

# MAGIC %md
# MAGIC Fibonacii Using Recursion

# COMMAND ----------

def fib(n):
    if n <=0: 
        return -1
    elif n == 1 or n==2:
        return 1
    else:
        return fib(n-1) + fib(n-2)

# COMMAND ----------

# MAGIC %md
# MAGIC Prime Number

# COMMAND ----------

def isPrime(n):
    if n<= 1:
        return False
    else:
        ans = True
        for i in range (2,(n/2)+1):
            if n%i == 0:
                ans = False
                break
        return ans

# COMMAND ----------

# MAGIC %md
# MAGIC String Revers

# COMMAND ----------

s = 'abcd'
print(s[::-1])

# COMMAND ----------

# MAGIC %md
# MAGIC Rolling Sum

# COMMAND ----------

data = [
    (1, "A", 10),
    (2, "A", 20),
    (3, "A", 30),
    (4, "A", 40),
    (5, "A", 50)
]


df = spark.createDataFrame(data, ["id", "category", "value"])
df.show()


# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *

windowSpec = Window.orderBy("id").rowsBetween(-2,0)


df_rolling_sum = df.withColumn("rolling_sum",sum("value").over(windowSpec))

df_rolling_sum.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Drop Duplicates from Data

# COMMAND ----------

data = [
    (1, "A", 10),
    (2, "A", 20),
    (3, "A", 30),
    (4, "A", 40),
    (5, "A", 50),
    (2, "A", 20),
    (3, "B", 30),
    (4, "A", 99),
]


df = spark.createDataFrame(data, ["id", "category", "value"])
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Find The Duplicates

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
df0 = df.groupBy("id").agg(count("id").alias("count")).filter(expr("count > 1 "))

df1 = df0.drop("count")

df0.show()
df1.show()

# COMMAND ----------

df.show()

df1 = df.drop_duplicates(["id"])

df1.show()

# COMMAND ----------

df3 = df.drop_duplicates(["id","category"])
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Applying using SQL

# COMMAND ----------

df.createOrReplaceTempView("table1")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1 AS(SELECT *,
# MAGIC ROW_NUMBER() OVER(PARTITION BY id order by id) as rn
# MAGIC from table1)
# MAGIC
# MAGIC Select * from cte1
# MAGIC where rn = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

df = spark.read.format("csv")\
  .option("inferSchema","true")\
  .option("header","true")\
  .option("path","/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/")\
  .option("mode","FAILFAST")\
.load()

df.show()

# COMMAND ----------

df = spark.read.format("csv")\
  .option("inferSchema","true")\
  .option("header","true")\
  .option("mode","FAILFAST")\
  .load("/Volumes/personal_catalog/default/spark_practice1/OutputWriteQuery/")

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Loading Data From 

# COMMAND ----------

data = [
    (1, "A", 10),
    (2, "A", 20),
    (3, "A", 30),
    (4, "A", 40),
    (5, "A", 50),
    (2, "A", 20),
    (3, "B", 30),
    (4, "A", 99)
]

mySchema = StructFeild([StructType("id",IntegerType(),True),StructType("name",StringType(),True),StructType("value",IntegerType(),True)])

df = spark.createDataframe(data,mySchema)

df.show()

# COMMAND ----------


