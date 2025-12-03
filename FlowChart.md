# ✅ Brazilian E-Commerce Modern Data Stack  
**Kaggle → CSV → Meltano → BigQuery → dbt → BigQuery ML → Power BI**

![Python](https://img.shields.io/badge/Python-3.10-blue)
![BigQuery](https://img.shields.io/badge/BigQuery-Warehouse-orange)
![dbt](https://img.shields.io/badge/dbt-Core-red)
![Meltano](https://img.shields.io/badge/Meltano-ELT-purple)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)
![Status](https://img.shields.io/badge/Status-Portfolio--Ready-brightgreen)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📌 Project Overview

This project implements a **full end-to-end analytics & machine learning pipeline** using the  
**Brazilian E-Commerce Public Dataset by Olist** on Kaggle.

You will learn how to:

✅ Generate Kaggle API credentials  
✅ Download Kaggle datasets using Python  
✅ Store raw CSV safely  
✅ Load raw data into BigQuery using Meltano  
✅ Verify CSV rows == BigQuery rows  
✅ Transform data using dbt  
✅ Train models using BigQuery ML  
✅ Visualize insights using Power BI  

This repository is suitable for:

- Data Engineer portfolios  
- Analytics Engineer portfolios  
- End-to-end ELT + ML + BI projects  
- Presentations to CEOs & technical teams  

---

## 🏗️ Architecture

```text
Kaggle API
   ↓
Python (download_kaggle.py)
   ↓
Raw CSV (data/)
   ↓
Meltano (tap-csv → target-bigquery)
   ↓
BigQuery Raw Tables (ecommerce.*)
   ↓
dbt (staging → dim → fact)
   ↓
BigQuery ML (Review Risk Model)
   ↓
Power BI Dashboards
```

---

## 🧰 Tech Stack

| Layer | Tools |
|--------|--------|
| Ingestion | Kaggle API, Python |
| ELT | Meltano |
| Warehouse | Google BigQuery |
| Transformations | dbt Core |
| Machine Learning | BigQuery ML |
| Visualization | Power BI |
| Version Control | Git, GitHub |
| Secrets | `.env`, `.gitignore` |

---

## 1️⃣ Conda Environment

```bash
conda env create -f eltn_environment.yml
conda activate eltn
```

---

## 2️⃣ Kaggle API Key

1. Go to https://www.kaggle.com/settings  
2. Scroll to **API** section  
3. Click **Create New API Token**  
4. Copy `username` and `key`

---

## 3️⃣ Setup `.env`

Create:

```text
.env
```

```env
KAGGLE_USERNAME=your_kaggle_username
KAGGLE_KEY=your_kaggle_key
GCP_PROJECT_ID=your_gcp_project
GOOGLE_APPLICATION_CREDENTIALS=/full/path/to/bq-key.json
```

---

## 4️⃣ Setup `.gitignore`

```gitignore
.env
*.json
.meltano/
data/*.csv
__pycache__/
.ipynb_checkpoints/
```

---

## 5️⃣ Download Kaggle CSV Files

Run:

```bash
python download_kaggle.py
```

Files saved into:

```text
data/
  olist_customers_dataset.csv
  olist_geolocation_dataset.csv
  olist_order_items_dataset.csv
  olist_order_payments_dataset.csv
  olist_order_reviews_dataset.csv
  olist_orders_dataset.csv
  olist_products_dataset.csv
  olist_sellers_dataset.csv
  product_category_name_translation.csv
```

---

## 6️⃣ Verify Local CSV Row Counts

Run:

```bash
python check_all_csvs.py
```

You should see output like:

```text
olist_customers_dataset.csv        →  99,441 rows, 5 columns
olist_orders_dataset.csv           → 100,000 rows, 8 columns
...
```

---

## 7️⃣ Create BigQuery Service Account

1. Go to GCP Console  
2. IAM & Admin → Service Accounts  
3. Create new → Role: **BigQuery Admin**  
4. Create JSON Key  
5. Save as:

```bash
/home/youruser/project/bq-key.json
```

Make sure this path matches `GOOGLE_APPLICATION_CREDENTIALS` in `.env` and `credentials_path` in `meltano.yml`.

---

## 8️⃣ Initialize Meltano

```bash
meltano init meltano_kaggle_csv
cd meltano_kaggle_csv
```

Add plugins:

```bash
meltano add extractor tap-csv
meltano add loader target-bigquery
```

---

## 9️⃣ Configure `meltano.yml`

### Extractor

```yaml
plugins:
  extractors:
    - name: tap-csv
      variant: meltanolabs
      config:
        files:
          - entity: olist_customers
            path: data/olist_customers_dataset.csv
            keys: [customer_id]

          - entity: olist_order_items
            path: data/olist_order_items_dataset.csv
            keys: [order_id, order_item_id]

          - entity: olist_order_payments
            path: data/olist_order_payments_dataset.csv
            keys: [order_id, payment_sequential]

          - entity: olist_order_reviews
            path: data/olist_order_reviews_dataset.csv
            keys: [review_id]

          - entity: olist_orders
            path: data/olist_orders_dataset.csv
            keys: [order_id]

          - entity: olist_products
            path: data/olist_products_dataset.csv
            keys: [product_id]

          - entity: olist_sellers
            path: data/olist_sellers_dataset.csv
            keys: [seller_id]

          - entity: olist_geolocation
            path: data/olist_geolocation_dataset.csv
            keys: [geolocation_zip_code_prefix]

          - entity: product_category_name_translation
            path: data/product_category_name_translation.csv
            keys: [product_category_name]

      select:
        - "*.*"
```

### Loader

```yaml
  loaders:
    - name: target-bigquery
      variant: z3z1ma
      config:
        project: your_gcp_project
        dataset: ecommerce
        location: US
        method: batch_job
        credentials_path: /full/path/to/bq-key.json
        denormalized: true
        flattening_enabled: true
        flattening_max_depth: 1
        upsert: false
        overwrite: false
        dedupe_before_upsert: false
```

---

## 🔟 Run ELT

```bash
meltano run tap-csv target-bigquery
```

---

## 1️⃣1️⃣ Verify BigQuery vs CSV Counts

In BigQuery:

```sql
SELECT 
  'olist_customers' AS table_name, COUNT(*) AS total_rows FROM ecommerce.olist_customers
UNION ALL
SELECT 
  'olist_geolocation', COUNT(*) FROM ecommerce.olist_geolocation
UNION ALL
SELECT 
  'olist_order_items', COUNT(*) FROM ecommerce.olist_order_items
UNION ALL
SELECT 
  'olist_order_payments', COUNT(*) FROM ecommerce.olist_order_payments
UNION ALL
SELECT 
  'olist_order_reviews', COUNT(*) FROM ecommerce.olist_order_reviews
UNION ALL
SELECT 
  'olist_orders', COUNT(*) FROM ecommerce.olist_orders
UNION ALL
SELECT 
  'olist_products', COUNT(*) FROM ecommerce.olist_products
UNION ALL
SELECT 
  'olist_sellers', COUNT(*) FROM ecommerce.olist_sellers
UNION ALL
SELECT 
  'product_category_name_translation', COUNT(*) FROM ecommerce.product_category_name_translation;
```

✅ All row counts should match CSV exactly.

---

## 1️⃣2️⃣ dbt Transformations (High Level)

```bash
pip install dbt-bigquery
dbt init olist_dbt
cd olist_dbt
```

Example folder layout:

```text
olist_dbt/
  models/
    staging/
      stg_orders.sql
      stg_customers.sql
      ...
    marts/
      dim_customers.sql
      dim_products.sql
      fct_orders.sql
  models/schema.yml
  dbt_project.yml
  profiles.yml
```

Run dbt:

```bash
dbt run
dbt test
```

---

## 1️⃣3️⃣ Example dbt Tests (`models/schema.yml`)

```yaml
version: 2

models:
  - name: dim_customers
    description: "Cleaned customer dimension from Olist data."
    columns:
      - name: customer_id
        description: "Unique customer identifier."
        tests:
          - not_null
          - unique

      - name: customer_unique_id
        description: "Customer natural key"
        tests:
          - not_null

  - name: fct_orders
    description: "Fact table combining orders, customers, items, payments and reviews."
    columns:
      - name: order_id
        tests:
          - not_null
          - unique
      - name: customer_id
        tests:
          - not_null
      - name: order_purchase_timestamp
        tests:
          - not_null
      - name: total_price
        tests:
          - not_null
```

---

## 1️⃣4️⃣ BigQuery ML – Predict Low Review Scores

### 1. Create Training Data (via dbt or SQL)

Assume you created a model `fct_orders` that aggregates:

- total_price  
- total_freight  
- items_count  
- review_score  

### 2. Train Logistic Regression Model

```sql
CREATE OR REPLACE MODEL `olist_analytics.order_low_score_model`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['is_low_score']
) AS
SELECT
  total_price,
  total_freight,
  items_count,
  CASE WHEN review_score <= 3 THEN 1 ELSE 0 END AS is_low_score
FROM `olist_analytics.fct_orders`;
```

### 3. Evaluate Model

```sql
SELECT *
FROM ML.EVALUATE(MODEL `olist_analytics.order_low_score_model`);
```

Key metrics to show:

- `precision`
- `recall`
- `accuracy`
- `roc_auc`

---

## 1️⃣5️⃣ Power BI Dashboards

### A. CEO View – Business KPIs

**Pages:**

1. **Revenue Overview**
   - Total Revenue
   - Orders Over Time
   - Revenue by State / City
   - Average Ticket Size
   - Card: Total Customers

2. **Customer Satisfaction**
   - Average Review Score
   - % Low-Score Orders (`review_score <= 3`)
   - Top 10 Products by Negative Reviews
   - Bar chart: Reviews by Score (1 to 5)

3. **Delivery Performance**
   - On-time vs Late Delivery Rate
   - Average Delivery Days by State
   - Correlation: Delivery Delay vs Review Score

---

### B. Technical View – Data & Model Quality

1. **Data Quality Monitoring**
   - Null rates per column (from dbt tests)
   - Number of failed dbt tests over time
   - Heatmap: columns vs % nulls

2. **ML Model Performance**
   - Accuracy, Precision, Recall from `ML.EVALUATE`
   - Confusion matrix visual
   - ROC Curve (imported from BigQuery ML evaluation export)

3. **Data Freshness**
   - Last load date from Meltano
   - Number of rows added per day
   - Alerts for missing partitions

---

## 1️⃣6️⃣ Git & GitHub Workflow

```bash
# Initialize Git
git init

# Stage files
git add .

# First commit
git commit -m "Initial commit: Kaggle → Meltano → BigQuery → dbt → ML → Power BI"

# Add remote (replace with your repo)
git remote add origin https://github.com/yourusername/Brazilian-E-Commerce-Public-Dataset-by-Olist.git

# Push
git branch -M main
git push -u origin main
```

---

## 1️⃣7️⃣ Resume-Ready Project Summary

**Project:** Brazilian E-Commerce Analytics & ML Pipeline (Olist)  
**Stack:** Python, Kaggle API, Meltano, BigQuery, dbt, BigQuery ML, Power BI, GitHub  

- Built a **fully automated ELT pipeline** from Kaggle to BigQuery using **Meltano** and **service accounts**.  
- Designed **dbt staging, dimension, and fact models** with data quality tests (uniqueness, not null, referential integrity).  
- Trained a **BigQuery ML logistic regression model** to predict **low review scores**, enabling proactive customer service.  
- Delivered two **Power BI dashboards**:
  - Executive view with revenue, delivery, and customer satisfaction KPIs  
  - Technical view with data quality, model performance, and pipeline freshness  
- Used **Git & GitHub** to version control all code, configs, and documentation.

---

## ✅ Status

- ✅ Data ingestion from Kaggle → CSV → BigQuery  
- ✅ Row/column validation between CSV and BigQuery  
- ✅ dbt modeling structure & example tests  
- ✅ Example BigQuery ML training & evaluation SQL  
- ✅ Dashboard design specification for Power BI  

🚀 This project is **portfolio-ready** and can be extended with more dbt models, ML models, or dashboards.


# 🚀 End-to-End Modern Data Pipeline with ML & Dashboard

This project demonstrates a **complete modern data engineering + analytics + machine learning pipeline**, from raw public data ingestion to executive-ready dashboards.

It is designed for:
- ✅ Data Engineering portfolios  
- ✅ Machine Learning projects  
- ✅ Business Intelligence demonstrations  
- ✅ CEO & Technical stakeholder presentations  

---

## 🖼️ Architecture Flow Diagram

> **Business → Engineering → ML → Dashboard in one continuous pipeline**

```mermaid
flowchart LR
    A[Kaggle Dataset] --> B[Pandas Processing]
    B --> C[CSV Files]
    
    C --> D[Meltano<br><b>Extract & Load</b>]
    D --> E[(BigQuery<br><b>Data Warehouse</b>)]
    
    E --> F[dbt<br><b>Transform</b>]
    F --> G[dbt-expectations<br><b>Data Quality</b>]
    
    G --> H[EDA<br><b>Exploration</b>]
    H --> I[Machine Learning]
    I --> J[Dashboard<br><b>Streamlit / Power BI / HTML</b>]
    
    style A fill:#E3F2FD,stroke:#1E88E5
    style B fill:#E8F5E9,stroke:#43A047
    style C fill:#F3E5F5,stroke:#8E24AA
    style D fill:#FFFDE7,stroke:#FBC02D
    style E fill:#E0F7FA,stroke:#00ACC1
    style F fill:#FBE9E7,stroke:#FB8C00
    style G fill:#FFEBEE,stroke:#E53935
    style H fill:#E1F5FE,stroke:#039BE5
    style I fill:#E8F5E9,stroke:#43A047
    style J fill:#F9FBE7,stroke:#9E9D24

