# End-to-End-Data-Warehouse-with-Databricks-Spark-SQL

This repository is a comprehensive, hands-on implementation of a Dimensional Data Model built entirely within Azure Databricks using Spark SQL.

The project covers the complete ETL (Extract, Transform, Load) lifecycle. It ingests raw data, processes it through a multi-stage Medallion Architecture (Bronze-Silver-Gold), and builds a final Star Schema optimized for analytics and business intelligence (OLAP).

It also serves as a practical guide to handling real-world data warehousing challenges, including incremental data loading and managing historical data with Slowly Changing Dimensions (SCDs).

Key Concepts & Project Recall (What I Learned)
This project is a deep dive into data warehousing, structured by the following key topics:

1. Foundations (OLTP vs. OLAP)

Recall: You learned the fundamental difference between OLTP (Online Transaction Processing) databases, which are high-speed, normalized systems for running the business (e.g., an e-commerce store), and OLAP (Online Analytical Processing) databases, which are denormalized systems for analyzing the business (e.g., a data warehouse).

2. The ETL Pipeline (Bronze-Silver-Gold)

Recall: You implemented a full ETL pipeline:

Bronze Layer: This is the initial Incremental Data Load. You built a script to efficiently ingest only new data from the source, rather than reloading the entire dataset each time.

Silver Layer: This is the cleaning and conforming layer. You used the powerful MERGE INTO (UPSERT) command to efficiently update existing records (like a customer's status) and insert new ones in a single, atomic transaction.

Gold Layer: This is the final, analytics-ready layer.

3. Dimensional Data Modeling (Gold Layer)

Recall: This is where you built the core data warehouse model:

Dimensions: Created the Dimension tables (e.g., dim_customer, dim_product), which contain the descriptive "who, what, where, when" attributes.

Facts: Created the Fact table (e.g., fact_sales), which contains the quantitative, numerical "how many, how much" measures and the foreign keys that link to the dimension tables.

4. Warehouse Schemas (Star vs. Snowflake)

Recall: You learned the two main design patterns. This project implements a Star Schema, where the Fact table links directly to all Dimension tables. This is compared to a Snowflake Schema, which is more normalized (e.g., a dim_location table might link to a separate dim_country table), saving space but often resulting in slower queries.

5. Handling Data History (Slowly Changing Dimensions)

Recall: This was a critical concept. You implemented the two most common ways to handle changes in dimension data:

SCD Type 1: Overwrite. When a customer's attribute changes (e.g., a typo correction), you simply UPDATE the existing record. No history is kept.

SCD Type 2: Track History. When a customer's attribute changes (e.g., they move to a new city), you expire the old record (e.g., is_current = false) and insert a new row with the new information. This allows you to accurately analyze historical data as it was at that point in time.
