# Databricks notebook source

sales_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/sales/")


# COMMAND ----------

from pyspark.sql.functions import to_date


sales_df = sales_df.withColumn("order_date", to_date("OrderDate", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

silver_sales_path = "dbfs:/mnt/blobstorage/sales/"

sales_df.write.mode("overwrite").parquet(silver_sales_path)

