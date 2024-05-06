# Databricks notebook source

store_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/store/")


# COMMAND ----------

store_df = store_df.withColumn("store_category", split(col("email_address"), "@")[1])

# COMMAND ----------

store_df.display()

# COMMAND ----------

from pyspark.sql.functions import split, current_date

silver_store_path = "dbfs:/mnt/blobstorage/store/"

store_df = spark.read.format("csv").option("header", "true").load(silver_store_path)

store_df = store_df.withColumn("store_category", split("store name", " ")[0]) \
                   .withColumn("created_at", current_date()) \
                   .withColumn("updated_at", current_date())




# COMMAND ----------

store_df.write.mode("overwrite").parquet(silver_store_path)
