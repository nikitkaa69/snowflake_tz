# Airline Data ELT Pipeline (Snowflake + Airflow)

## Project Overview
This project implements an end-to-end **ELT (Extract, Load, Transform)** pipeline using **Apache Airflow** for orchestration and **Snowflake** for data warehousing.

The pipeline processes airline flight data, moving it from raw CSV files through a multi-layered architecture (RAW -> CORE -> MARTS), ensuring data quality, deduplication, and security.

---

## Architecture & Data Flow

The project follows a modern **Bronze-Silver-Gold** architecture:

1.  **Ingestion:** Airflow loads local CSV data into a Snowflake **Internal Stage** and then into the **RAW** layer.
2.  **Transformation (CORE):** A Stored Procedure processes data from `RAW` using **Snowflake Streams** (CDC). It normalizes data into `PASSENGERS`, `PILOTS`, `AIRPORTS`, and `FLIGHTS` tables, handling deduplication and surrogate keys.
3.  **Aggregation (MARTS):** A final procedure aggregates flight statistics into the `DAILY_AIRPORT_STATS` table for BI reporting.
4.  **Orchestration:** All steps are managed by a single Airflow DAG using the `SQLExecuteQueryOperator`.

---

## Implemented Features

| Requirement | Implementation Detail |
| :--- | :--- |
| **Data Staging** | Used Snowflake Internal Stage (`@AIRLINE_FLIGHTS_STAGE`) for CSV upload. |
| **Pipeline Layers** | Implemented **RAW** (Source), **CORE** (Normalized), and **MARTS** (Aggregated). |
| **Orchestration** | **Airflow DAG** (`snowflake_load_dag`) automates the flow: Upload -> Copy -> Call Procedures. |
| **CDC / Streams** | Used `AIRLINE_FLIGHTS_STREAM` to process only new incoming data from RAW to CORE. |
| **Audit Logging** | Created `AUDIT_LOG` table. Procedures calculate `SQLROWCOUNT` and log every run status. |
| **Time Travel** | SQL scripts demonstrate `UNDROP TABLE` (DDL) and `SELECT AT(TIMESTAMP)` (DML) recovery. |
| **Security** | Implemented **Row-Level Security** using a Mapping Table and `SECURE VIEW` (`FACT_FLIGHTS_SECURE`). |
| **BI Report** | Connected **Google Looker Studio** to Snowflake to visualize Flight stats. |
---
![Untitled_Report](https://github.com/user-attachments/assets/d6436679-aba3-4722-9756-010d8a796757)
