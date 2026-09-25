# E-Commerce ELT Data Pipeline 🛒 

👉 [**View Live Looker Studio Dashboard**](https://lookerstudio.google.com/u/0/reporting/84f8573d-2fc7-4f84-b377-eab82d2ace04/page/dMnuF)

![Architecture Visualization](https://i.imgur.com/your-dashboard-screenshot-link.jpg)

<img width="2418" height="1802" alt="image" src="https://github.com/user-attachments/assets/51f77dff-1d88-40e2-a321-11b8966f4238" />

## Project Overview

This project is an end-to-end Data Engineering pipeline built to automatically Extract, Load, and Transform (ELT) real-world e-commerce sales data. The architecture revolves around the **Modern Data Stack**, emphasizing modularity, data quality, and cloud-native integrations. 

The pipeline ingests the [Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) featuring 100,000 anonymized orders, extracts it via `dlt` (data load tool), and loads it into **Google BigQuery**. The data is then transformed inside BigQuery utilizing `dbt` adhering strictly to the **Medallion Architecture** (Bronze, Silver, Gold). The entire process is orchestrated sequentially via a local **Apache Airflow** server.

## 🛠 Technology Stack
- **Data Source:** API/Local CSV (Kaggle API)
- **Ingestion (EL):** Python and `dlt` (data load tool)
- **Data Warehouse:** Google BigQuery
- **Transformation (T):** `dbt` (data build tool) & SQL
- **Orchestration:** Apache Airflow
- **Visualization:** Looker Studio (connected directly to BigQuery Mrt tables)

## 🏗 Architecture Details

1. **Extraction & Loading (`dlt`)**
   - The initial layer relies on Python's `dlt` library. Due to memory constraints, large Kaggle CSVs (over 40MB) are batched, chunked using `pandas`, and streamed into BigQuery.
   - `dlt` dynamically infers the data schema (Strings, Timestamps, Integers) and automatically initializes the `ecommerce_bronze` datasets in GCP.

2. **Transformation Pipeline (`dbt`)**
   We structured the logic flow into 3 layers (Medallion Architecture):
   - **`Bronze` (Raw Data):** Casts data models directly from underlying `dlt` tables as SQL Views.
   - **`Silver` (Cleansed/Integrated):** Flattens nested JSON metrics and executes high-level logical joins (e.g., tying orders, items, and customers together).
   - **`Gold` (Business Marts):** Final aggregations containing top-line analytics (e.g., `mart_sales_by_city` counting geographic revenue efficiency).

3. **Orchestration (Apache Airflow)**
   - The final pipeline workflow is controlled by Apache Airflow. A configured DAG (`dags/ecommerce_pipeline_dag.py`) manages the dependency between Extraction/Loading and the `dbt` transformations, ensuring the Medallion pipeline never fires off dirty/incomplete underlying data streams.

---

## 🚀 How to Run Locally

### 1. Requirements & Google Cloud Authentication
Ensure you have Python 3.9+ and Google Cloud CLI (`gcloud`) installed.

Set up the environment and install dependencies:
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Verify your active Mac terminal has Application Default Credentials linked to your GCP Project:
```bash
gcloud auth application-default login
```
*(Ensure your BigQuery Project ID is properly set within `transform/profiles.yml` and `.dlt/secrets.toml`)*

### 2. Manual Execution (Optional)
Run the dataset extraction to Google BigQuery:
```bash
python ingestion/pipeline.py
```
After successful extraction, execute all data quality tests and transformations:
```bash
cd transform
dbt build
```

### 3. Executing via Airflow DAG
To execute the pipeline end-to-end automatically:
```bash
# Point Airflow toward this repository
export AIRFLOW_HOME=$(pwd)

# Boot the Airflow WebServer and Scheduler locally
airflow standalone
```
Navigate to `http://localhost:8080` (password generated in Terminal), and manually trigger the `ecommerce_daily_pipeline` DAG!
