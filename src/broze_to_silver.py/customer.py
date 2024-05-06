# Databricks notebook source
dbutils.fs.mount(
    source = 'wasbs://bronze@mahalakshmi33.blob.core.windows.net/',
    mount_point = '/mnt/blobstorage' ,
    extra_configs = {'fs.azure.account.key.mahalakshmi33.blob.core.windows.net':'3nsqbQ1E32FWtm7W6zre+JXPhAjxR8qMty2i/gS1NCAoM9sSokP3q304vYb2GsX8IqmowscP9w6i+AStTomrrQ=='}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/blobstorage')

# COMMAND ----------

bronze_customer_path = '/mnt/blobstorage/sales_view_devtst/customer'

# COMMAND ----------

silver_customer_path = '/mnt/blobstorage/silver/sales_view/customer'

# COMMAND ----------

customer_df = spark.read.option("header", "true").csv('dbfs:/mnt/blobstorage/customer/')

# COMMAND ----------

customer_df.display()

# COMMAND ----------



def camel_to_snake(column_name):
    import re
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', column_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


# COMMAND ----------

customer_df = customer_df.toDF(*[camel_to_snake(col) for col in customer_df.columns])

# COMMAND ----------

from pyspark.sql.functions import split,col

customer_df = customer_df.withColumn("name_split", split(col("name"), " "))\
                         .withColumn("first_name", col("name_split")[0])\
                         .withColumn("last_name", col("name_split")[1])\
                         .drop("name_split")

# COMMAND ----------

from pyspark.sql.functions import split


customer_df = customer_df.withColumn("domain", split(col("email _id"), "@")[1])


# COMMAND ----------

from pyspark.sql.functions import when, col

gender_map = {"M": "Male", "F": "Female"}
customer_df = customer_df.withColumn("gender", when(col("gender") == "M", "Male").otherwise("Female"))

# COMMAND ----------

customer_df = customer_df.withColumn("joining_date_split", split(col("joining _date"), " "))
customer_df = customer_df.withColumn("joining _date", col("joining_date_split")[0])\
                         .withColumn("joining_time", col("joining_date_split")[1])\
                         .drop("joining_date_split")

# COMMAND ----------

from pyspark.sql.functions import to_date

customer_df = customer_df.withColumn("joining _date", to_date("joining _date", "yyyy-MM-dd"))

# COMMAND ----------

customer_df = customer_df.withColumn("expenditure_status", when(col("spent") < 200, "MINIMUM").otherwise("MAXIMUM"))


# COMMAND ----------

customer_df = customer_df.select("customer _id", "first_name", "last_name", "domain", "gender", "joining _date", "joining_time", "expenditure_status")



# COMMAND ----------

customer_df.write.mode("overwrite").parquet(silver_customer_path)





