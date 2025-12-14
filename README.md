# Zomato Sales ETL Pipeline 

This repository contains the code, notebooks, and documentation for a complete end-to-end data engineering pipeline built on **Zomato sales data** using **Databricks**, **Delta Lake**, and **AWS S3**.

The project follows the **Medallion Architecture** (Bronze → Silver → Gold) to ingest, clean, transform, and aggregate raw sales data into analytics-ready tables. All visualizations and dashboards are built directly within **Databricks** using its native visualization tools and dashboards.

## Project Overview

The goal of this project is to demonstrate a scalable, production-grade data pipeline that:
- Ingests raw CSV data from Zomato sales
- Stores raw data immutably (Bronze layer)
- Cleans and standardizes the data (Silver layer)
- Creates business-level aggregated tables (Gold layer)
- Enables interactive visualizations and dashboards within Databricks

## Architecture
Raw CSV (Zomato Sales)
↓
AWS S3 Bucket → External Location in Databricks
↓
Bronze Layer (Raw Delta Table - Unity Catalog)
↓
Silver Layer Pipeline (PySpark Transformations)
↓
Silver Delta Tables (Cleaned & Standardized)
↓
Gold Layer Aggregations (Analytical Summary Tables)
↓
Databricks Visualizations & Dashboards
text## Medallion Layers

### 1. Bronze Layer (Data Ingestion)
**Purpose**: Store raw data as-is for auditability, data lineage, and replay capability.

**Steps Performed**:
- Created an AWS S3 bucket to store the raw `zomato_sales.csv` file
- Configured an IAM Role with necessary S3 permissions
- Set up an **External Location** in Databricks Unity Catalog pointing to the S3 bucket
- Loaded raw data into a **Bronze Delta Table** using Unity Catalog

**Table**: `bronze.zomato_sales_raw`

### 2. Silver Layer (Data Cleaning & Standardization)
**Purpose**: Produce clean, trusted, and analytics-ready data.

**Pipeline**:
- Load data from Bronze table
- Perform cleaning and transformations using PySpark
- Remove duplicates
- Load into Silver schema

**Key Transformations**:
- Trimmed whitespace in `product_name`
- Handled NULL values in `product_name`
- Explicit data type conversions for `order_id`, `product_id`, `quantity`, `price`, `store_id`
- Generated derived timestamp fields: `order_timestamp`, `order_day`
- Added `cleaned_product_name` column

**Table**: `silver.zomato_sales_cleaned`

### 3. Gold Layer (Business Aggregations)
**Purpose**: Create optimized, business-level aggregated tables for reporting and analysis.

**Analytical Tables Created**:

1. **Daily Sales Summary**
   - Order day
   - Total revenue
   - Total items sold
   - Total orders

2. **Sales by Store**
   - Store revenue ranking
   - Items sold per store
   - Total orders per store

3. **Top Products**
   - Cleaned product name
   - Total quantity sold
   - Total revenue

4. **Fact Revenue Table**
   - Clean fact table with key metrics
   - Joined with product and store dimension tables
   - Optimized for query and dashboard performance

**Schema**: `gold.*`

### 4. Visualizations & Dashboards (Databricks Native)
All business insights and reporting are delivered through **Databricks SQL Dashboards** and notebook visualizations, covering the following key areas:

1. **Executive Summary**
   - Total revenue
   - Total orders
   - Average revenue per store
   - Revenue by store
   - Sales distribution by product category
   - Top 15 products by revenue

2. **Product Insights**
   - Top 15 products by revenue
   - High-revenue product highlights
   - Product revenue trend over time
   - Top 15 products by quantity
   - Revenue distribution by product

3. **Store Performance**
   - Store revenue ranking
   - Highest revenue store
   - Average revenue per store
   - Store order trend over time

4. **Time Trends & Insights**
   - Daily revenue trend
   - Monthly revenue trend
   - Peak order hours analysis

These visualizations are built using Databricks SQL queries on the Gold layer tables and published as interactive dashboards for stakeholders.

## Technologies Used
- **Cloud Storage**: AWS S3
- **Data Platform**: Databricks (Unity Catalog, Delta Lake, Dashboards, Notebook Visualizations)
- **Language**: PySpark (SQL & DataFrame API)
- **Architecture**: Medallion (Bronze-Silver-Gold)


text## How to Run
1. Set up AWS S3 bucket and upload `zomato_sales.csv`
2. Configure Databricks External Location with appropriate IAM role
3. Run notebooks in sequence: Bronze → Silver → Gold
4. Create and publish Databricks dashboards using the provided queries on Gold tables

## Key Learnings
- Importance of immutable raw storage (Bronze)
- Effective data quality handling in Silver layer
- Performance optimization through aggregated Gold tables
- Native, collaborative, and version-controlled visualizations within Databricks

---

**Author**: Pritesh Bhere  
**Date**: December 2025  
**Platform**: Databricks + AWS + PySpark
