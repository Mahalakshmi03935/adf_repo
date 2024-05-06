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

# COMMAND ----------



products_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/products/")
store_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/store/")
sales_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/sales/")
customer_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/customer/")




# COMMAND ----------

products_df.display()

# COMMAND ----------

from pyspark.sql.functions import when

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

# COMMAND ----------

from pyspark.sql.functions import to_date


sales_df = sales_df.withColumn("order_date", to_date("OrderDate", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

silver_sales_path = "dbfs:/mnt/blobstorage/sales/"

sales_df.write.mode("overwrite").parquet(silver_sales_path)


# COMMAND ----------

from pyspark.sql.functions import col

# Join product and store tables to get required columns
gold_df = products_df.join(store_df, products_df["store_id"] == store_df["store ID"])

# Join with customer_sales to get additional columns
gold_df = gold_df.join(customer_df, gold_df["product ID"] == customer_df["order ID"])

# Select relevant columns
gold_df = gold_df.select(
    products_df["product ID"],
    products_df["product name"],
    products_df["product code"],
    products_df["description"],
    products_df["category_id"],
    products_df["price"],
    products_df["stock_quantity"],
    products_df["supplier_id"],
    store_df["store ID"],
    store_df["store name"],
    store_df["location"],
    store_df["manager_name"],
    store_df["phone_number"],
    customer_df["Customer Id"],
    customer_df["Name"],
    customer_df["Email Id"],
    customer_df["address"],
    customer_df["gender"],
    customer_df["age"],
    customer_df["Joining Date"],
    customer_df["registered"],
    customer_df["orders"],
    customer_df["spent"],
    customer_df["job"],
    customer_df["hobbies"],
    customer_df["is_married"]
)

# Define the path for the gold sales analysis
gold_sales_analysis_path = "/path/to/gold/sales/analysis.parquet"

# Write to gold layer
gold_df.write.mode("overwrite").parquet(gold_sales_analysis_path)


# COMMAND ----------


gold_df.write.mode("overwrite").parquet(gold_sales_analysis_path)

# COMMAND ----------

sales_df.display()

# COMMAND ----------

sales_df = spark.read.option("header", "true").csv("dbfs:/mnt/blobstorage/sales")

sales_df.display()


# COMMAND ----------


