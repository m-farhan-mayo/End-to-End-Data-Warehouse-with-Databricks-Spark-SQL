# Databricks notebook source
# MAGIC %md
# MAGIC ### **Dim Customers**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimCustomers
# MAGIC AS
# MAGIC WITH rem_dup AS
# MAGIC (
# MAGIC SELECT 
# MAGIC   DISTINCT(customer_id),
# MAGIC   customer_email,
# MAGIC   customer_name,
# MAGIC   Customer_Name_Upper
# MAGIC FROM datamodeling.silver.silver_table
# MAGIC )
# MAGIC SELECT * ,
# MAGIC       row_number() OVER (ORDER BY customer_id) as DimCustomerKey
# MAGIC FROM rem_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim Products**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimProducts
# MAGIC AS
# MAGIC WITH rem_dup AS
# MAGIC (
# MAGIC SELECT 
# MAGIC   DISTINCT(product_id),
# MAGIC   product_name,
# MAGIC   product_category
# MAGIC FROM 
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC SELECT * , row_number() OVER (ORDER BY product_id) as DimProductKey
# MAGIC FROM rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.gold.dimproducts

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim Payments**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimPayments
# MAGIC WITH rem_dup AS 
# MAGIC (
# MAGIC SELECT 
# MAGIC   DISTINCT(payment_type)
# MAGIC FROM datamodeling.silver.silver_table
# MAGIC ) 
# MAGIC SELECT *, row_number() OVER (ORDER BY payment_type) as DimPaymentKey FROM rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.gold.DimPayments

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim Region**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimRegions
# MAGIC WITH rem_dup AS 
# MAGIC (
# MAGIC SELECT 
# MAGIC   DISTINCT(country)
# MAGIC FROM datamodeling.silver.silver_table
# MAGIC ) 
# MAGIC SELECT *, row_number() OVER (ORDER BY country) as DimRegionKey FROM rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.gold.dimregions

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dim Sales**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.DimSales
# MAGIC AS
# MAGIC SELECT 
# MAGIC  row_number() OVER (ORDER BY order_id) as DimSaleKey,
# MAGIC  order_id,
# MAGIC  order_date,
# MAGIC  customer_id,
# MAGIC  customer_name,
# MAGIC  customer_email,
# MAGIC  product_id,
# MAGIC  product_name,
# MAGIC  product_category,
# MAGIC  payment_type,
# MAGIC  country,
# MAGIC  last_updated,
# MAGIC  Customer_Name_Upper,
# MAGIC  processDate
# MAGIC FROM
# MAGIC   datamodeling.silver.silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.gold.dimsales

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FACT TABLE**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.FactSales
# MAGIC AS
# MAGIC SELECT 
# MAGIC   S.DimSaleKey,
# MAGIC   C.DimCustomerKey,
# MAGIC   P.DimProductKey,
# MAGIC   R.DimRegionKey,
# MAGIC   PY.DimPaymentKey,
# MAGIC   F.quantity,
# MAGIC   F.unit_price
# MAGIC FROM 
# MAGIC   datamodeling.silver.silver_table F
# MAGIC LEFT JOIN 
# MAGIC   datamodeling.gold.dimcustomers C
# MAGIC   ON F.customer_id = C.customer_id
# MAGIC LEFT JOIN 
# MAGIC   datamodeling.gold.dimproducts P
# MAGIC   ON F.product_id = P.product_id
# MAGIC LEFT JOIN 
# MAGIC   datamodeling.gold.dimregions R
# MAGIC   ON F.country = R.country
# MAGIC LEFT JOIN 
# MAGIC   datamodeling.gold.dimpayments PY
# MAGIC   ON F.payment_type = PY.payment_type
# MAGIC LEFT JOIN 
# MAGIC   datamodeling.gold.dimsales S
# MAGIC   ON F.order_id = S.order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.gold.FactSales

# COMMAND ----------

