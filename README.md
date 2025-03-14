Two Python files, DataMount_DesignImplementation.py and Analytical_Queries.py are included.
Azure Blob Storage and Databricks were used to store the data and to model a datawarehouse respectively.
A Data Dictionary is also included in this README.md file for reference.

Additionally if the raw-dataset is needed, here is the URL of the blob storage
[Download CSV](https://ecommercetarget.blob.core.windows.net/ecommerce-data?sp=rl&st=2025-03-14T21:51:29Z&se=2025-06-12T05:51:29Z&spr=https&sv=2024-11-04&sr=c&sig=xWgcF8CdO45iIje77Sof170Hhe9cFlGuuwgOcgHFiNw%3D)


Data Dictionary


1. Customers (customers.csv)

customer_id (STRING): ID of the consumer who made the purchase.

customer_unique_id (STRING): Unique ID of the consumer.

customer_zip_code_prefix (STRING): Zip Code of consumer’s location.

customer_city (STRING): Name of the City from where the order is made.

customer_state (STRING): State Code from where the order is made (e.g., São Paulo - SP).



2. Geolocation (geolocation.csv)

geolocation_zip_code_prefix (STRING): First 5 digits of the Zip Code.

geolocation_lat (FLOAT): Latitude.

geolocation_lng (FLOAT): Longitude.

geolocation_city (STRING): City name.

geolocation_state (STRING): State name.



3. Orders (orders.csv)

order_id (STRING): Unique ID of the order made by the consumers.

customer_id (STRING): ID of the consumer who made the purchase.

order_status (STRING): Status of the order (e.g., delivered, shipped).

order_purchase_timestamp (TIMESTAMP): Timestamp of the purchase.

order_delivered_carrier_date (TIMESTAMP): Delivery date at which the carrier made the delivery.

order_delivered_customer_date (TIMESTAMP): Date at which the customer received the product.

order_estimated_delivery_date (TIMESTAMP): Estimated delivery date of the products.



4. Order Items (order_items.csv)

order_id (STRING): Unique ID of the order.

order_item_id (INTEGER): Unique ID given to each item ordered in the order.

product_id (STRING): Unique ID given to each product.

seller_id (STRING): Unique ID of the seller registered in Target.

shipping_limit_date (TIMESTAMP): Date before which the ordered product must be shipped.

price (FLOAT): Actual price of the products ordered.

freight_value (FLOAT): Price rate at which a product is delivered.



5. Payments (payments.csv)

order_id (STRING): Unique ID of the order.

payment_sequential (INTEGER): Sequence of the payments made in case of EMI.

payment_type (STRING): Mode of payment used (e.g., Credit Card).

payment_installments (INTEGER): Number of installments in case of EMI purchase.

payment_value (FLOAT): Total amount paid for the purchase order.



6. Products (products.csv)

product_id (STRING): Unique identifier for the product.

product_category_name (STRING): Name of the product category.

product_name_length (INTEGER): Length of the string specifying the product name.

product_description_length (INTEGER): Length of the product description.

product_photos_qty (INTEGER): Number of photos of each product.

product_weight_g (FLOAT): Weight of the products ordered in grams.

product_length_cm (FLOAT): Length of the products in centimeters.

product_height_cm (FLOAT): Height of the products in centimeters.

product_width_cm (FLOAT): Width of the product in centimeters.



7. Sellers (sellers.csv)

seller_id (STRING): Unique ID of the seller.

seller_zip_code_prefix (STRING): Zip Code of the seller’s location.

seller_city (STRING): City of the seller.

seller_state (STRING): State Code (e.g., São Paulo - SP).

