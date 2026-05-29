# 🛒 End-to-End E-Commerce Data Pipeline (Modern Data Stack)

## 📌 Project Overview
This project is a fully automated, end-to-end Data Engineering pipeline built to extract, process, and visualize e-commerce transaction data. It utilizes the Modern Data Stack (MDS) to pull fake store data via an API, enforce strict data quality contracts, transform the data into a star schema, and serve it to a live executive dashboard.

## 🏗️ Architecture
*(Insert a screenshot of your Draw.io or Excalidraw architecture diagram here. Example: API -> Airflow -> DuckDB -> dbt -> Streamlit)*

## 🛠️ Technology Stack
* **Orchestration:** Apache Airflow (Astronomer / TaskFlow API)
* **Processing & Extraction:** Python, Pandas, REST APIs
* **Data Warehouse:** DuckDB (Local OLAP columnar database)
* **Data Modeling (ELT):** dbt (Data Build Tool)
* **Visualization:** Streamlit, Plotly
* **Infrastructure:** Docker

## 🚀 Key Engineering Highlights
* **Map-Reduce Data Quality Gating:** Engineered a dynamic parallel validation step in Airflow that acts as a "Data Contract." It actively scans incoming CSVs for null IDs or negative revenue, quarantining corrupted files before they reach the database.
* **Idempotent Data Loading:** Built sequential, localized write logic for DuckDB to prevent concurrent write locks, ensuring the pipeline can be re-run indefinitely without duplicating records.
* **Modular dbt Transformations:** Upgraded from raw SQL to dbt, defining raw tables as formal `sources` and materializing clean `dim_customers` and `fct_daily_sales` models for optimized downstream querying.
* **Containerized Development:** Utilized the Astro CLI to containerize the Airflow environment, ensuring absolute consistency across local and production environments while utilizing `.dockerignore` to optimize build speeds.

## 📂 Project Structure
```text
├── dags/                       # Airflow DAGs (Extract, Transform, Load)
├── include/
│   └── tasks/                  # Modular Python execution scripts
├── fakeshop_dbt/               # dbt project (models, sources, profiles)
├── dashboard.py                # Streamlit executive dashboard
├── fakeshop_warehouse.db       # DuckDB local data warehouse
└── requirements.txt            # Python dependencies