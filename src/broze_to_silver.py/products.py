# Databricks notebook source


products_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/products/")

# COMMAND ----------

products_df.display()



# COMMAND ----------

from pyspark.sql.functions import when
from pyspark.sql.functions import col

products_df = products_df.withColumn("sub_category",
                                   when(col("category_id") == 1, "phone")
                                   .when(col("category_id") == 2, "laptop")
                                   .when(col("category_id") == 3, "playstation")
                                   .when(col("category_id") == 4, "e-device")
                                   .otherwise("other"))


# COMMAND ----------

silver_product_path = "dbfs:/mnt/blobstorage/products/"

# COMMAND ----------

products_df.write.mode("overwrite").parquet(silver_product_path)
