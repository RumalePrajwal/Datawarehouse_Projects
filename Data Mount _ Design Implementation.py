# Databricks notebook source
# Defining Storage Credentials
storage_account_name = "ecommercetarget"
container_name = "ecommerce-data"
storage_account_key = "KHFl77VFYWbDfSIE3dPj6NW2Zg2WMY2Mxh2/fLoke8KJKMhPXRIs+4MueYkriv2Ew0Ap2LCxZodA+AStR+xeWA=="

# Mounting Azure Blob Storage
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point="/mnt/azure_storage",
    extra_configs={f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_key}
)

# Verifying Mount
display(dbutils.fs.ls("/mnt/azure_storage"))




# COMMAND ----------

# Writing raw-data into staging delta tables
df_stg_customers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/customers.csv")

df_stg_customers.write.format("delta").mode("overwrite").saveAsTable("staging_customers")

df_stg_geolocations = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/geolocation.csv")

df_stg_geolocations.write.format("delta").mode("overwrite").saveAsTable("staging_geolocations")

df_stg_orders = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/orders.csv")

df_stg_orders.write.format("delta").mode("overwrite").saveAsTable("staging_orders")

df_stg_order_items = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/order_items.csv")

df_stg_order_items.write.format("delta").mode("overwrite").saveAsTable("staging_order_items")

df_stg_payments = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/payments.csv")

df_stg_payments.write.format("delta").mode("overwrite").saveAsTable("staging_payments")

df_stg_products = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/products.csv").withColumnRenamed("product category", "product_category")

df_stg_products.write.format("delta").mode("overwrite").saveAsTable("staging_products")

df_stg_sellers = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("dbfs:/mnt/azure_storage/sellers.csv")

df_stg_sellers.write.format("delta").mode("overwrite").saveAsTable("staging_sellers")

# COMMAND ----------

from pyspark.sql.functions import coalesce, col, lit, monotonically_increasing_id, current_date, year, month, dayofweek
from pyspark.sql.types import IntegerType, DecimalType, StringType
from delta.tables import DeltaTable

# --------------------- DIMENSION TABLES ---------------------

# Create `dim_customers` with SCD Type 2
dim_customers = DeltaTable.forName(spark, "dim_customers")

df_new_customers = spark.sql("""
SELECT customer_id,
       customer_unique_id,
       COALESCE(customer_zip_code_prefix, '00000') AS customer_zip_code_prefix,
       current_date() AS effective_date,
       NULL AS end_date
FROM staging_customers
""")

dim_customers.alias("old") \
    .merge(df_new_customers.alias("new"), "old.customer_id = new.customer_id") \
    .whenMatchedUpdate(condition="old.customer_zip_code_prefix != new.customer_zip_code_prefix", 
                       set={"old.end_date": "current_date()"}) \
    .whenNotMatchedInsert(values={
        "customer_id": "new.customer_id",
        "customer_unique_id": "new.customer_unique_id",
        "customer_zip_code_prefix": "new.customer_zip_code_prefix",
        "effective_date": "current_date()",
        "end_date": lit(None)
    }) \
    .execute()

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
dim_products = DeltaTable.forName(spark, "dim_products")

df_new_products = spark.sql("""
SELECT product_id,
       COALESCE(product_category, 'UNKNOWN') AS product_category,
       COALESCE(product_weight_g, 0) AS product_weight_g,
       COALESCE(product_length_cm, 0) AS product_length_cm,
       COALESCE(product_height_cm, 0) AS product_height_cm,
       COALESCE(product_width_cm, 0) AS product_width_cm,
       current_date() AS effective_date,
       NULL AS end_date
FROM staging_products
""")

dim_products.alias("old") \
    .merge(df_new_products.alias("new"), "old.product_id = new.product_id") \
    .whenMatchedUpdate(condition="""
        old.product_category != new.product_category OR
        old.product_weight_g != new.product_weight_g OR
        old.product_length_cm != new.product_length_cm OR
        old.product_height_cm != new.product_height_cm OR
        old.product_width_cm != new.product_width_cm
    """, 
    set={"old.end_date": "current_date()"}) \
    .whenNotMatchedInsert(values={
        "product_id": "new.product_id",
        "product_category": "new.product_category",
        "product_weight_g": "new.product_weight_g",
        "product_length_cm": "new.product_length_cm",
        "product_height_cm": "new.product_height_cm",
        "product_width_cm": "new.product_width_cm",
        "effective_date": "current_date()",
        "end_date": lit(None)
    }) \
    .execute()

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

# COMMAND ----------

# --------------------- FACT TABLES ---------------------

# Create `fact_sales` 
df_fact_sales = spark.sql("""
SELECT o.order_id,
       ord.order_key,   
       c.customer_key,
       p.product_key,
       s.seller_key,
       g.geolocation_key AS customer_geolocation_key, 
       d.date_key AS order_date_key,
       o.order_status,
       o.order_purchase_timestamp,
       o.order_delivered_customer_date,
       oi.shipping_limit_date,
       oi.price,
       oi.freight_value
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

# Create `fact_payments`
df_fact_payments = spark.sql("""
SELECT p.order_id,
       p.payment_sequential,
       COALESCE(p.payment_type, 'UNKNOWN') AS payment_type,
       COALESCE(p.payment_installments, 0) AS payment_installments,
       COALESCE(p.payment_value, 0) AS payment_value,
       d.date_key AS payment_date_key  
FROM staging_payments p
LEFT JOIN dim_dates d ON p.payment_value IS NOT NULL 
""").withColumn("payment_key", monotonically_increasing_id())

df_fact_payments.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("fact_payments")