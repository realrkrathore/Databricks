# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *

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

# COMMAND ----------

schema = "date String, sales Integer"

# COMMAND ----------

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df_with_quarters = df.withColumn("year",year("date"))\
    .withColumn("quarter",quarter(col("date")))

df_with_quarters.show()

# COMMAND ----------

from pyspark.sql.functions import sum

quarterlySales = (
    df_with_quarters.groupBy(
        "year",
        "quarter"
    ).agg(
        sum("sales").alias("total_sales")
    )
)

display(quarterlySales)

# COMMAND ----------

pivotedTable = quarterlySales.groupBy("year").pivot("quarter",[1,2]).sum("total_sales")
pivotedTable.show()

# COMMAND ----------

from pyspark.sql.functions import col

result = pivotedTable.withColumn(
    "percent_change",
    ((col("2") - col("1")) / col("1")) * 100
)

display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------

data = [('Rudra','math',79),
('Rudra','eng',60),
('Shivu','math', 68),
('Shivu','eng', 59),
('Anu','math', 65),
('Anu','eng',80)]

# COMMAND ----------

mySchema ="Name String,Sub String,Marks int"

# COMMAND ----------

df=spark.createDataFrame(data,mySchema)
df.show()

# COMMAND ----------

ansDf = df.groupBy("Name").pivot("Sub").sum("Marks")
ansDf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

data = [(1,'Arul','Chennai','2023-01-01'   ),
(1,'Arul','Bangalore','2023-02-01' ),
(2,'Sam','Chennai','2023-01-01'    ),
(3,'manish','patna','2023-01-01'   ),
(3,'manish','patna','2023-03-15'   ),
(3,'manish','patna','2023-02-27'   )]

schema = "id Integer, name String, location String,date String"

df = spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------


df.createOrReplaceTempView("myView")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from myView

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_temp AS (
# MAGIC   SELECT *,
# MAGIC     RANK() OVER(PARTITION BY id
# MAGIC      ORDER BY date DESC) AS RN
# MAGIC   FROM myView
# MAGIC )
# MAGIC SELECT id, name, location, date
# MAGIC FROM cte_temp
# MAGIC WHERE RN = 1

# COMMAND ----------

# MAGIC %md
# MAGIC ---

# COMMAND ----------


data= [(1, 'Arul',      'SQL'   ),
(1 ,'Arul' ,     'Spark' ),
(2, 'Bhumica' ,  'SQL'   ),
(2 ,'Bhumica' ,  'Spark' )]

schema= 'id int, name String,course String'

df= spark.createDataFrame(data=data,schema=schema)
df.show()


# COMMAND ----------


df.createOrReplaceTempView("table")

# COMMAND ----------

df = spark.sql("""
          select id, name,
          array_join(collect_list(course),',') as courses
          from table group by id,name
          """)
df.createOrReplaceView()

# COMMAND ----------

spark.sql("""
          select id,name,explode(split(courses,',')) as course from table2
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

schema=['id','name','sal','mngr_id']

manager_df= spark.createDataFrame(data=data,schema=schema)
manager_df.createOrReplaceTempView("manager_tbl")
spark.sql("""
select * from manager_tbl
""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC Select mngr_id, AVG(sal) AS average_sal
# MAGIC from manager_tbl
# MAGIC group by  mngr_id
# MAGIC order by average_sal DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ----

# COMMAND ----------

flights_data = [(1,'Flight1' , 'Delhi' , 'Hyderabad'),
                (1,'Flight2' , 'Hyderabad' , 'Kochi'),
                (1,'Flight3' , 'Kochi' , 'Mangalore'),
                (2,'Flight1' , 'Mumbai' , 'Ayodhya'),
                (2,'Flight2' , 'Ayodhya' , 'Gorakhpur')
                ]

_schema = "cust_id int, flight_id string , origin string , destination string"

df_flight = spark.createDataFrame(data = flights_data , schema= _schema)
df_flight.show()

# COMMAND ----------

df_flight.createOrReplaceTempView("table")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cte_temp AS (
# MAGIC     SELECT *,
# MAGIC         RANK() OVER (
# MAGIC             PARTITION BY cust_id
# MAGIC             ORDER BY some_column
# MAGIC         ) AS rn
# MAGIC     FROM table
# MAGIC )
# MAGIC SELECT
# MAGIC     cust_id,
# MAGIC     MAX(rn) AS dest_rn,
# MAGIC     MIN(rn) AS origin_rn
# MAGIC FROM cte_temp
# MAGIC GROUP BY cust_id

# COMMAND ----------


