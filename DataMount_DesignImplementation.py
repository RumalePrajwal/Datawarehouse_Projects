# Databricks notebook source
# Defining Storage Credentials
storage_account_name = "ecommercetarget"
container_name = "ecommerce-data"
storage_account_key = "KHFl77VFYWbDfSIE3dPj6NW2Zg2WMY2Mxh2/fLoke8KJKMhPXRIs+4MueYkriv2Ew0Ap2LCxZodA+AStR+xeWA=="

# Mounting Azure Blob Storage
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/data_storage",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# Verifying Mount
display(dbutils.fs.ls("/mnt/data_storage"))

# COMMAND ----------

# Writing raw-data into staging delta tables
df_stg_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/customers.csv")

df_stg_customers.write.format("delta").mode("overwrite").saveAsTable("staging_customers")

df_stg_geolocations = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/geolocation.csv")

df_stg_geolocations.write.format("delta").mode("overwrite").saveAsTable("staging_geolocations")

df_stg_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/orders.csv")

df_stg_orders.write.format("delta").mode("overwrite").saveAsTable("staging_orders")

df_stg_order_items = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/order_items.csv")

df_stg_order_items.write.format("delta").mode("overwrite").saveAsTable("staging_order_items")

df_stg_payments = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/payments.csv")

df_stg_payments.write.format("delta").mode("overwrite").saveAsTable("staging_payments")

df_stg_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/products.csv").withColumnRenamed("product category", "product_category")

df_stg_products.write.format("delta").mode("overwrite").saveAsTable("staging_products")

df_stg_sellers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/data_storage/sellers.csv")

df_stg_sellers.write.format("delta").mode("overwrite").saveAsTable("staging_sellers")

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, lit, monotonically_increasing_id, current_date, year, month, dayofweek
from pyspark.sql.types import IntegerType, DecimalType, StringType
from delta.tables import DeltaTable
from pyspark.sql.types import DateType

# --------------------- DIMENSION TABLES ---------------------

# Create `dim_customers` with SCD Type 2
df_dim_customers = spark.sql("""
SELECT DISTINCT customer_id,
       customer_unique_id,
       COALESCE(customer_zip_code_prefix, '00000') AS customer_zip_code_prefix,
       current_date() AS effective_date,
       NULL AS end_date  
FROM staging_customers
""").withColumn("customer_key", monotonically_increasing_id()) \
  .withColumn("end_date", lit(None).cast(DateType()))  

df_dim_customers.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("dim_customers")


# Create `dim_geolocation`
df_dim_geolocation = spark.sql("""
SELECT DISTINCT geolocation_zip_code_prefix AS zip_code_prefix,
       COALESCE(geolocation_lat, 0.0) AS latitude,
       COALESCE(geolocation_lng, 0.0) AS longitude,
       COALESCE(geolocation_city, 'UNKNOWN') AS city,
       COALESCE(geolocation_state, 'UNKNOWN') AS state
FROM staging_geolocations
""").withColumn("geolocation_key", monotonically_increasing_id())

df_dim_geolocation.write.format("delta").mode("overwrite").saveAsTable("dim_geolocation")

# Create `dim_products` with SCD Type 2
df_dim_products = spark.sql("""
SELECT DISTINCT product_id,
       COALESCE(product_category, 'UNKNOWN') AS product_category,
       COALESCE(product_weight_g, 0) AS product_weight_g,
       COALESCE(product_length_cm, 0) AS product_length_cm,
       COALESCE(product_height_cm, 0) AS product_height_cm,
       COALESCE(product_width_cm, 0) AS product_width_cm,
       current_date() AS effective_date
FROM staging_products
""").withColumn("product_key", monotonically_increasing_id()) \
  .withColumn("end_date", lit(None).cast(DateType()))  

df_dim_products.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("dim_products")


# Create `dim_sellers` with SCD Type 2
df_dim_sellers = spark.sql("""
SELECT DISTINCT seller_id,
       COALESCE(seller_zip_code_prefix, '00000') AS seller_zip_code_prefix,
       current_date() AS effective_date,
       NULL AS end_date  
FROM staging_sellers
""").withColumn("seller_key", monotonically_increasing_id()) \
  .withColumn("end_date", lit(None).cast(DateType()))  

df_dim_sellers.write.format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("dim_sellers")


# Create `dim_orders`
df_dim_orders = spark.sql("""
SELECT DISTINCT order_id,
       COALESCE(order_status, 'UNKNOWN') AS order_status
FROM staging_orders
""").withColumn("order_key", monotonically_increasing_id())

df_dim_orders.write.format("delta").mode("overwrite").saveAsTable("dim_orders")

# Create `dim_dates`
df_dim_dates = spark.sql("""
SELECT DISTINCT order_purchase_timestamp AS full_date,
       YEAR(order_purchase_timestamp) AS year,
       MONTH(order_purchase_timestamp) AS month,
       DAY(order_purchase_timestamp) AS day,
       DAYOFWEEK(order_purchase_timestamp) AS weekday
FROM staging_orders
""").withColumn("date_key", monotonically_increasing_id())

df_dim_dates.write.format("delta").mode("overwrite").saveAsTable("dim_dates")

# --------------------- FACT TABLES ---------------------

# Create `fact_sales`
df_fact_sales = spark.sql("""
SELECT o.order_id,
       ord.order_key,   
       c.customer_key,
       p.product_key,
       s.seller_key,
       g.geolocation_key,  
       d.date_key AS order_date_key,
       COALESCE(o.order_status, 'UNKNOWN') AS order_status,
       o.order_purchase_timestamp,
       COALESCE(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS order_delivered_customer_date,
       oi.shipping_limit_date,
       COALESCE(oi.price, 0) AS price,
       COALESCE(oi.freight_value, 0) AS freight_value
FROM staging_orders o
LEFT JOIN staging_order_items oi ON o.order_id = oi.order_id
LEFT JOIN dim_orders ord ON o.order_id = ord.order_id
LEFT JOIN dim_customers c ON o.customer_id = c.customer_id
LEFT JOIN dim_products p ON oi.product_id = p.product_id
LEFT JOIN dim_sellers s ON oi.seller_id = s.seller_id
LEFT JOIN dim_dates d ON o.order_purchase_timestamp = d.full_date
LEFT JOIN dim_geolocation g ON c.customer_zip_code_prefix = g.zip_code_prefix
""")

df_fact_sales.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("fact_sales")



# COMMAND ----------

# Create fact_payments
df_fact_payments = spark.sql("""
SELECT p.order_id,
       p.payment_sequential,
       COALESCE(p.payment_type, 'UNKNOWN') AS payment_type,
       COALESCE(p.payment_installments, 0) AS payment_installments,
       COALESCE(p.payment_value, 0) AS payment_value,
       d.date_key AS payment_date_key  -- Linking Payments with Dates
FROM staging_payments p
JOIN staging_orders o ON p.order_id = o.order_id
JOIN dim_dates d ON o.order_purchase_timestamp = d.full_date  -- Match Payment Date
""").withColumn("payment_key", monotonically_increasing_id())

df_fact_payments.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("fact_payments")


# COMMAND ----------

