# Databricks notebook source
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

