# Databricks notebook source
from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("groupby").getOrCreate()

# COMMAND ----------

# Similar to SQL GROUP BY clause, PySpark groupBy() transformation that is used to group rows that have the same values in specified columns into summary rows. It allows you to perform aggregate functions on groups of rows, rather than on individual rows, enabling you to summarize data and generate aggregate statistics.

# COMMAND ----------

df= spark.table("samples.bakehouse.sales_transactions")
display(df)

# COMMAND ----------

# Data frame

# COMMAND ----------

df1=df.groupBy(df["product"]).count()
display(df1)

# COMMAND ----------

# SQL

# COMMAND ----------

df.createOrReplaceTempView("sales")
df1= spark.sql("select product, count(*) as count from sales group by product")
display(df1)

# COMMAND ----------

from pyspark.sql.functions import sum
df1=df.groupBy(df["product"]).agg((sum(df["totalprice"])).alias("total_price"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import mean
df1=df.groupBy(df["product"]).agg((mean(df["totalprice"])).alias("total_price"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import max
df1=df.groupBy(df["product"]).agg((max(df["totalprice"])).alias("total_price"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import min
df1=df.groupBy(df["product"]).agg((min(df["totalprice"])).alias("total_price"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import avg
df1=df.groupBy(df["product"]).agg((avg(df["totalprice"])).alias("total_price"))
display(df1)

# COMMAND ----------

from pyspark.sql.functions import sum
df1=df.groupBy(df["product"],df["paymentmethod"]).agg((sum(df["totalprice"])).alias("total_price"),(sum(df["quantity"])).alias("quantity"))
display(df1)