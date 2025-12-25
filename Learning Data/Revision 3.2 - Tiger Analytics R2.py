# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC CIR - Click per impression Percentage

# COMMAND ----------

data = [
    (123, 'impression', '2025-03-01 10:00:00'),
    (123, 'impression', '2025-03-01 10:01:00'),
    (123, 'click', '2025-03-01 10:02:00'),
    (234, 'impression', '2025-03-01 10:03:00'),
    (234, 'click', '2025-03-01 10:04:00')
]


# COMMAND ----------

schema = "app_id integer,event_type string,timestamp string"

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 1: Count impressions and clicks per app_id/event_type
# MAGIC

# COMMAND ----------

df_count = df.groupBy("app_id","event_type").count()
df_count.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2: Pivot event_type to columns

# COMMAND ----------

df_pivot = df_count.groupBy("app_id").pivot("event_type").sum("count")
df_pivot.show()

# COMMAND ----------

df_final = df_pivot.withColumn("CTR",(col("click")/col("impression"))*100)
df_final.show()

# COMMAND ----------

# MAGIC %md
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC Q : PRINT the newest Record For Each Name

# COMMAND ----------

data = [(1,'Arul','Chennai','2023-01-01'   ),
(1,'Arul','Bangalore','2023-02-01' ),
(2,'Sam','Chennai','2023-01-01'    ),
(3,'manish','patna','2023-01-01'   ),
(3,'manish','patna','2023-03-15'   ),
(3,'manish','patna','2023-02-27'   )]

# COMMAND ----------

schema = 'id integer, name string, location string, date string'

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL :
# MAGIC
# MAGIC with cte_temp AS(  
# MAGIC   Select *,  
# MAGIC   Rank() Over(partitionBy id orderBy date Desc) as RN  
# MAGIC   from  table  
# MAGIC )  
# MAGIC
# MAGIC Select id,name,location,date  
# MAGIC from cte_temp  
# MAGIC where RN = 1  
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Solution

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col

# COMMAND ----------

df2 = df.withColumn("RN",rank().over(Window.partitionBy("id").orderBy(col("date").desc())))
df2.show()

# COMMAND ----------

df3 = df2.select("id","name","location","date").filter("RN = 1")
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC Q2> Merge Course Coulumn data and Seprate it using ','

# COMMAND ----------

data = [(1, 'Arul', 'SQL'),
        (1, 'Arul', 'Spark'),
        (2, 'Bhumica', 'SQL'),
        (2, 'Bhumica', 'Spark')]

schema = ['id', 'name', 'course']

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC #create or replace temporary view ans_table AS 
# MAGIC SELECT
# MAGIC   id,
# MAGIC   name,
# MAGIC   concat_ws(',',collect_list(course)) AS course
# MAGIC FROM tdf
# MAGIC GROUP BY id,name 

# COMMAND ----------

 result = df.groupBy("id","name").agg(concat_ws(',', collect_set(col("course"))).alias('courses'))
#.select('id','name','courses')
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Doing Visa-Versa Of Above Question

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from ans_table

# COMMAND ----------

# MAGIC %md
# MAGIC Spliting Course Column

# COMMAND ----------

# MAGIC %sql
# MAGIC Select id , name,
# MAGIC explode(split(course,',')) as course
# MAGIC from ans_table

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Solution

# COMMAND ----------

result.show()

# COMMAND ----------

from pyspark.sql.functions import explode
result2 = result.select('id','name',explode(split("courses",",")).alias("course"))
result2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC find Average Salary Of Each Manager (Average of Salary of Employees Working under him)

# COMMAND ----------

data = [
    (10, 'Anil', 50000, 18),
    (11, 'Vikas', 75000, 16),
    (12, 'Nisha', 40000, 18),
    (13, 'Nidhi', 60000, 17),
    (14, 'Priya', 80000, 18),
    (15, 'Mohit', 45000, 18),
    (16, 'Rajesh', 90000, 10),
    (17, 'Raman', 55000, 16),
    (18, 'Sam', 65000, 17)
]
schema=['id', 'name', 'sal', 'mngr_id']

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Solution

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tdf

# COMMAND ----------

# MAGIC %sql
# MAGIC Select mngr_id,
# MAGIC avg(sal) as Mngr_avg
# MAGIC from tdf
# MAGIC group by mngr_id

# COMMAND ----------

# MAGIC %md
# MAGIC Apply Join to manager_name table , Get Manager id,Name,mngr_avg Salary

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Approach

# COMMAND ----------

df.show()

# COMMAND ----------

result = df.groupBy("mngr_id").avg("sal").alias('mngr_avg')
result.show()

# COMMAND ----------

result = df.groupBy("mngr_id").agg(avg("sal").alias('mngr_avg'))
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC -----

# COMMAND ----------

# MAGIC %md
# MAGIC Q : Convert a Single Column into Multiple Column

# COMMAND ----------

data=[
('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math', 68),
('Shivu','eng', 59),
('Anu','math', 65),
('Anu','eng',80)
]
schema="Name string,Sub string,Marks int"

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df2= df.groupBy('Name').agg(collect_list('Marks').alias("Marks_List"))
df2.show()

# COMMAND ----------

df_result = df2.select('Name',col("Marks_List")[0].alias('Maths'),col("Marks_List")[1].alias("Eng"))
df_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC PySpark Soltion 2

# COMMAND ----------

df2 =  df.groupBy("Name").pivot("Sub").sum("Marks")
df2.show()

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tdf

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH marks_agg AS (
# MAGIC   SELECT
# MAGIC     name,
# MAGIC     collect_list(Marks) AS Marks
# MAGIC   FROM tdf
# MAGIC   GROUP BY name
# MAGIC )
# MAGIC SELECT
# MAGIC   name,
# MAGIC   Marks[0] AS Maths,
# MAGIC   Marks[1] AS Eng
# MAGIC FROM marks_agg

# COMMAND ----------

# MAGIC %md
# MAGIC SQL Soltion 2

# COMMAND ----------

# MAGIC %sql
# MAGIC Select name,
# MAGIC SUM(CASE WHEN sub like 'math' THEN MARKS ELSE 0 END) AS MATH,
# MAGIC SUM(CASE WHEN sub like 'eng' THEN MARKS ELSE 0 END) AS ENG
# MAGIC FROM tdf
# MAGIC GROUP BY name
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC Q > Find Origin and Final Destination

# COMMAND ----------

flights_data = [
    (1, 'Flight1', 'Delhi', 'Hyderabad'),
    (1, 'Flight2', 'Hyderabad', 'Kochi'),
    (1, 'Flight3', 'Kochi', 'Mangalore'),
    (2, 'Flight1', 'Mumbai', 'Ayodhya'),
    (2, 'Flight2', 'Ayodhya', 'Gorakhpur')
]

_schema = "cust_id int, flight_id string, origin string, destination string"


# COMMAND ----------

df = spark.createDataFrame(flights_data,_schema)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark 1

# COMMAND ----------

o_df = df.alias("o")
d_df = df.alias("d")

result = (o_df
          .join(d_df, o_df.destination == d_df.origin)
          .select(o_df.origin, d_df.destination)
)

result.show()

# COMMAND ----------

final_result = result.join(df,result.origin == df.origin).select( df.cust_id,df.flight_id,result.origin,result.destination)

final_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import *
df2 = df.withColumn("Rn",row_number().over(Window.partitionBy("cust_id").orderBy("flight_id")))
df2.show()

# COMMAND ----------

df3 = df2.groupBy("cust_id").agg(
    min("Rn").alias("start"),
    max("Rn").alias("end")
)

df3.show()

# COMMAND ----------

df_answer = df2.join(df3, on = (df2.cust_id == df3.cust_id)).drop(df3.cust_id)
df_answer.show()

# COMMAND ----------

result = df_answer.groupBy("cust_id").agg(
    max(when ( col("Rn") == col("start"), df_answer.origin)).alias("origin"),
    max(when ( col("Rn") == col("end"), df_answer.destination)).alias("destination")
)

result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tdf

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tdf o;
# MAGIC Select * from tdf d;

# COMMAND ----------

# MAGIC %md 
# MAGIC partial soltion below

# COMMAND ----------

# MAGIC %sql
# MAGIC select o.origin,d.destination
# MAGIC from tdf o join tdf d 
# MAGIC on o.destination = d.origin;

# COMMAND ----------

# MAGIC %md
# MAGIC SQL
# MAGIC

# COMMAND ----------

with cte1 as
(
select cid,origin from flights 
except
select cid,destination as origin from flights 
),cte2 as
(select cid,destination from flights 
except
select cid,origin as destination from flights
) select c1.cid,c1.origin,c2.destination from cte1 c1
  inner join cte2 c2
  on c1.cid = c2.cid

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC Find The Count of New Customer in Each Month

# COMMAND ----------


data = [
    ("2021-01-01", "C1", 20),
    ("2021-01-01", "C2", 30),
    ("2021-02-01", "C1", 10),
    ("2021-02-01", "C3", 15),
    ("2021-03-01", "C5", 19),
    ("2021-03-01", "C4", 10),
    ("2021-04-01", "C3", 13),
    ("2021-04-01", "C5", 15),
    ("2021-04-01", "C6", 10)
]

columns = ["order_date", "customer", "qty"]

df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from tdf

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date,count(distinct customer) as number_of_cust,qty from (
# MAGIC   select *,
# MAGIC   ROW_NUMBER() OVER(PARTITION BY customer ORDER BY order_date) as RN
# MAGIC   from tdf
# MAGIC )
# MAGIC
# MAGIC WHERE RN = 1
# MAGIC group by order_date

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_temp AS (
# MAGIC   SELECT *,
# MAGIC     ROW_NUMBER() OVER(
# MAGIC       PARTITION BY customer
# MAGIC       ORDER BY order_date
# MAGIC     ) AS RN
# MAGIC   FROM tdf
# MAGIC )
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   COUNT(DISTINCT customer) AS number_of_cust,
# MAGIC   SUM(qty) AS total_qty
# MAGIC FROM cte_temp
# MAGIC WHERE RN = 1
# MAGIC GROUP BY order_date

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ----

# COMMAND ----------

# MAGIC %md
# MAGIC calculate the percentage differenece between sales in Quarter 1 and Quarter 2 of Each Year

# COMMAND ----------

data = [
    ("2010-01-02", 500),
    ("2010-02-03", 1000),
    ("2010-03-04", 1000),
    ("2010-04-05", 1000),
    ("2010-05-06", 1500),
    ("2010-06-07", 1000),
    ("2010-07-08", 1000),
    ("2010-08-09", 1000),
    ("2011-10-10", 1000),
    ("2011-01-02", 500),
    ("2011-02-03", 1000),
    ("2011-03-04", 1000),
    ("2011-04-05", 1000),
    ("2011-05-06", 1550),
    ("2011-06-07", 1100),
    ("2011-07-08", 1100),
    ("2011-08-09", 1000),
]

schema = ["date", "sales"]

df = spark.createDataFrame(data,schema)

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pyspark

# COMMAND ----------

df2 = df.withColumn("Year",year("date"))\
    .withColumn("Quarter",quarter("date"))
df2.show()

# COMMAND ----------

quarterly_sales = df2.groupBy("Year","Quarter").agg(sum("sales").alias("total_sales"))
quarterly_sales.show()

# COMMAND ----------

pivot_sales = quarterly_sales.groupBy("year").pivot("Quarter",[1,2]).sum('total_sales')

pivot_sales = pivot_sales.withColumnRenamed("1","Q1_sales")\
    .withColumnRenamed("2","Q2_sales")

pivot_sales.show()

# COMMAND ----------

from pyspark.sql.functions import col, when

result = pivot_sales.withColumn(
    "percentage_diff",
    when(
        (col("Q1_sales").isNotNull()) & (col("Q2_sales").isNotNull()) & (col("Q1_sales") != 0),
        ((col("Q2_sales") - col("Q1_sales")) / col("Q1_sales")) * 100
    ).otherwise(None)
)
display(result)

# COMMAND ----------

df.createOrReplaceTempView("tdf")

# COMMAND ----------

# MAGIC %md
# MAGIC SQL

# COMMAND ----------


