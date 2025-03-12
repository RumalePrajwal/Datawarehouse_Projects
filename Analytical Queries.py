# Databricks notebook source
import matplotlib.pyplot as plt

# SQL Query in Python

# Top 5 Best Selling Products by Revenue
df_best_products = spark.sql("""
SELECT p.product_category,
       ROUND(SUM(s.price)) AS total_revenue
FROM fact_sales s
JOIN dim_products p ON s.product_key = p.product_key
GROUP BY p.product_category
ORDER BY total_revenue DESC
LIMIT 5
""").toPandas()  # Converting to Pandas DataFrame

# Bar Chart
plt.figure(figsize=(10, 5))
plt.barh(df_best_products["product_category"], df_best_products["total_revenue"], color="skyblue")
plt.xlabel("Total Revenue")
plt.ylabel("Product Category")
plt.title("Top 5 Best-Selling Products by Revenue")
plt.gca().invert_yaxis()
plt.show()

# COMMAND ----------


# Customer Retention
df_customer_retention = spark.sql("""
WITH CustomerOrders AS (
    SELECT c.customer_unique_id,
           COUNT(f.order_id) AS total_orders
    FROM fact_sales f
    JOIN dim_customers c ON f.customer_key = c.customer_key
    GROUP BY c.customer_unique_id
)
SELECT 
    COUNT(CASE WHEN total_orders > 1 THEN 1 END) AS repeat_customers,
    COUNT(CASE WHEN total_orders = 1 THEN 1 END) AS one_time_customers
FROM CustomerOrders;
""").toPandas()

# Checking if DataFrame is empty before accessing values
if not df_customer_retention.empty:
    repeat_customers = df_customer_retention["repeat_customers"].values[0] if not df_customer_retention["repeat_customers"].isna().any() else 0
    one_time_customers = df_customer_retention["one_time_customers"].values[0] if not df_customer_retention["one_time_customers"].isna().any() else 0

    # Plot Pie Chart
    plt.figure(figsize=(8, 8))
    plt.pie([repeat_customers, one_time_customers], labels=["Repeat Customers", "One-Time Customers"], autopct="%1.1f%%", startangle=140, colors=["green", "red"])
    plt.title("Customer Retention: Repeat vs. One-Time Customers")
    plt.show()
else:
    print("No data available for Customer Retention.")

# COMMAND ----------


# Payment Method Preferences
df_payment_methods = spark.sql("""
SELECT p.payment_type,
       ROUND(COUNT(p.order_id)) AS total_transactions
FROM fact_payments p
GROUP BY p.payment_type
ORDER BY total_transactions DESC
""").toPandas()

# Pie Chart
plt.figure(figsize=(8, 8))
plt.pie(df_payment_methods["total_transactions"], labels=df_payment_methods["payment_type"], autopct="%1.1f%%", startangle=140, colors=["gold", "skyblue", "lightcoral", "lightgreen"])
plt.title("Payment Method Preferences")
plt.show()

# COMMAND ----------


# Top 10 Sellers by Revenue
df_top_sellers = spark.sql("""
SELECT s.seller_id,
       g.city,
       g.state,
       ROUND(SUM(f.price)) AS total_revenue
FROM fact_sales f
JOIN dim_sellers s ON f.seller_key = s.seller_key
JOIN dim_geolocation g ON s.seller_zip_code_prefix = g.zip_code_prefix
GROUP BY s.seller_id, g.city, g.state
ORDER BY total_revenue DESC
LIMIT 10
""").toPandas()

# Horizontal Bar Chart
plt.figure(figsize=(10, 5))
plt.barh(df_top_sellers["seller_id"], df_top_sellers["total_revenue"], color="purple")
plt.xlabel("Total Revenue")
plt.ylabel("Seller ID")
plt.title("Top 10 Sellers by Revenue")
plt.gca().invert_yaxis()
plt.show()

# COMMAND ----------

import matplotlib.pyplot as plt

# Sales Revenue by State
df_revenue_by_state = spark.sql("""
SELECT g.state,
       COUNT(f.order_id) AS total_orders,
       ROUND(SUM(f.price), 0) AS total_revenue
FROM fact_sales f
JOIN dim_geolocation g ON f.customer_geolocation_key = g.geolocation_key
GROUP BY g.state
ORDER BY total_revenue DESC
""").toPandas()

# Visualization (Bar Chart for Sales by State)
plt.figure(figsize=(12, 5))
plt.bar(df_revenue_by_state["state"], df_revenue_by_state["total_revenue"], color="blue")
plt.xlabel("State")
plt.ylabel("Total Revenue")
plt.title("Total Revenue by State")
plt.xticks(rotation=90)
plt.show()
