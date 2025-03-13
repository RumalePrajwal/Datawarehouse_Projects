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

# Average Delivery Time by State

df_delivery_time = spark.sql("""
SELECT g.state,
       ROUND(AVG(DATEDIFF(f.order_delivered_customer_date, f.order_purchase_timestamp)), 2) AS avg_delivery_time
FROM fact_sales f
JOIN dim_customers c ON f.customer_key = c.customer_key
JOIN dim_geolocation g ON c.customer_zip_code_prefix = g.zip_code_prefix
WHERE f.order_delivered_customer_date IS NOT NULL
GROUP BY g.state
ORDER BY avg_delivery_time DESC
""").toPandas()

# Bar Chart
plt.figure(figsize=(12, 5))
plt.bar(df_delivery_time["state"], df_delivery_time["avg_delivery_time"], color="orange")
plt.xlabel("State")
plt.ylabel("Average Delivery Time (Days)")
plt.title("Average Delivery Time by State")
plt.xticks(rotation=90)
plt.show()


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

# Yearly Sales Growth
df_yearly_sales = spark.sql("""
SELECT d.year,
       ROUND(SUM(f.price), 0) AS total_revenue,
       LAG(ROUND(SUM(f.price), 0)) OVER (ORDER BY d.year) AS previous_year_revenue,
       ROUND(
          ((ROUND(SUM(f.price), 0) - LAG(ROUND(SUM(f.price), 0)) OVER (ORDER BY d.year)) 
          / LAG(ROUND(SUM(f.price), 0)) OVER (ORDER BY d.year)) * 100, 2
       ) AS growth_rate
FROM fact_sales f
JOIN dim_dates d ON f.order_date_key = d.date_key
GROUP BY d.year
ORDER BY d.year
""").toPandas()

# Line Chart
plt.figure(figsize=(10, 5))
plt.plot(df_yearly_sales["year"], df_yearly_sales["total_revenue"], marker="o", linestyle="-", color="green")
plt.xlabel("Year")
plt.ylabel("Total Revenue")
plt.title("Yearly Sales Growth")
plt.grid()
plt.show()
