# 🚀 End-to-End Snowflake Data Engineering Pipeline Project

### *Real-Time Medallion Architecture using Snowpipe, Streams, Tasks & SCD Type 2*

<div align="center">

<img src="https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white" />
<img src="https://img.shields.io/badge/AWS%20S3-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white" />
<img src="https://img.shields.io/badge/SQL-336791?style=for-the-badge&logo=postgresql&logoColor=white" />
<img src="https://img.shields.io/badge/Status-Production%20Ready-00C853?style=for-the-badge" />

<br/>

![Architecture](https://img.shields.io/badge/Architecture-Medallion-blue?style=flat-square)
![Pipeline](https://img.shields.io/badge/Pipeline-RealTime-success?style=flat-square)
![SCD2](https://img.shields.io/badge/SCD-Type%202-orange?style=flat-square)
![Automation](https://img.shields.io/badge/Automation-Snowpipe-important?style=flat-square)

</div>

---

# 📋 Table of Contents

- Project Overview
- Architecture
- Technologies Used
- Project Workflow
- Medallion Architecture
- Pipeline Components
- SCD Type 2 Implementation
- Gold Layer Tables
- Key Features
- SQL Concepts Used
- File Structure
- Challenges Faced
- Future Improvements
- Author

---

# 🔭 Project Overview

This project is a complete end-to-end real-time data engineering pipeline built using Snowflake and AWS S3 following the Medallion Architecture approach.

The pipeline automatically ingests JSON customer data from AWS S3 into Snowflake using Snowpipe, processes incremental changes using Streams and Tasks, applies SCD Type 2 logic, and creates analytical Gold layer tables for reporting and business analytics.

The project demonstrates real-world enterprise-level data engineering concepts including:

- Real-Time Data Ingestion
- Snowpipe Automation
- Streams & Tasks
- SCD Type 2
- Medallion Architecture
- Incremental Data Processing
- Data Warehousing Concepts
- Fact & Dimension Modeling

---

# 🏗️ Architecture

```text
AWS S3
   ↓
Snowpipe
   ↓
Bronze Layer (Raw JSON Data)
   ↓
Streams
   ↓
Tasks (SCD Type 2 Processing)
   ↓
Silver Layer (Cleaned & Historical Data)
   ↓
Gold Layer (Fact & Dimension Tables)
   ↓
Analytics / Reporting
```

---

# 🛠️ Technologies Used

| Technology | Purpose |
|---|---|
| Snowflake | Cloud Data Warehouse |
| AWS S3 | Data Lake Storage |
| SQL | Data Processing |
| Snowpipe | Real-Time Ingestion |
| Streams | Change Data Capture |
| Tasks | Workflow Automation |
| JSON | Semi-Structured Data |
| Medallion Architecture | Data Layering |

---

# 📂 Medallion Architecture

## 🥉 Bronze Layer

The Bronze layer stores raw JSON data exactly as received from the source system.

### Components
- External Stage
- File Format
- Raw Table
- Snowpipe

### Purpose
- Raw data storage
- Audit tracking
- Source preservation

---

## 🥈 Silver Layer

The Silver layer transforms and cleans raw data while applying SCD Type 2 logic.

### Features
- Data cleaning
- Historical tracking
- Incremental processing
- Hash comparison
- Stream processing

### Main Table
```sql
CUSTOMER_FINAL
```

---

## 🥇 Gold Layer

The Gold layer contains analytics-ready business tables.

### Tables Created

| Table | Purpose |
|---|---|
| DIM_CUSTOMER_GOLD | Dimension table |
| FACT_CUSTOMER_METRICS_GOLD | Fact table |
| DM_CUSTOMER_FULL_DETAILS | Combined analytics table |

---

# ⚡ Project Workflow

## Step 1 — Database & Schema Creation

Created:
- Database
- Schema
- File Format
- External Stage

```sql
CREATE OR REPLACE DATABASE snow_project;
```

---

## Step 2 — AWS S3 Integration

Connected Snowflake with AWS S3 bucket using external stage.

```sql
CREATE OR REPLACE STAGE MY_S3_STAGE
URL='s3://project-bucket-datalake';
```

---

## Step 3 — Snowpipe Configuration

Configured Snowpipe for automatic ingestion of JSON files.

### Features
- Auto-ingestion
- Continuous loading
- Real-time pipeline

---

## Step 4 — Stream Creation

Created Stream object for Change Data Capture (CDC).

```sql
CREATE OR REPLACE STREAM customer_stream
ON TABLE customer_raw;
```

---

## Step 5 — SCD Type 2 Processing

Implemented Slowly Changing Dimension Type 2 logic using MERGE statement.

### Features
- Historical tracking
- Effective start/end dates
- Current flag management
- Hash comparison

---

# 🔄 SCD Type 2 Logic

The project tracks historical changes in customer records.

### Columns Used

| Column | Purpose |
|---|---|
| RECORD_HASH | Change detection |
| EFF_START_DT | Record start date |
| EFF_END_DT | Record expiry date |
| IS_CURRENT | Active record flag |

---

# 📊 Gold Layer Tables

## 1. DIM_CUSTOMER_GOLD

Stores descriptive customer information.

### Columns
- Customer Name
- Email
- City
- Source System
- Load Dates

---

## 2. FACT_CUSTOMER_METRICS_GOLD

Stores numerical business metrics.

### Metrics
- Credit Rating
- Rating Score
- Load Year

---

## 3. DM_CUSTOMER_FULL_DETAILS

Combined analytical table containing both dimension and fact data.

### Purpose
- Reporting
- Dashboarding
- Analytics

---

# 📅 Date Intelligence Columns

The project extracts multiple date attributes for analytics.

### Added Columns
- LOAD_YEAR
- LOAD_MONTH
- LOAD_DAY
- LOAD_DAY_NAME
- LOAD_QUARTER

---

# 🔥 Key Features

- Real-Time Data Ingestion
- Automated ETL Pipeline
- Medallion Architecture
- SCD Type 2
- Incremental Processing
- Historical Data Tracking
- Stream-Based CDC
- Task Dependency Chain
- Analytics-Ready Gold Layer

---

# 🧮 SQL Concepts Used

| Concept | Usage |
|---|---|
| MERGE | SCD Type 2 |
| HASH | Change detection |
| STREAM | CDC |
| TASK | Automation |
| PIPE | Auto ingestion |
| CASE WHEN | Business logic |
| VARIANT | JSON handling |
| TIMESTAMP | Audit tracking |

---

# 📈 Pipeline Automation

The pipeline runs automatically using Snowflake Tasks.

### Task Flow

```text
PROCESS_SCD2_TASK
        ↓
LOAD_GOLD_TABLES_TASK
```

---

# 📁 File Structure

```text
Snowflake-Data-Engineering-Project/
│
├── SQL/
│   └── pipeline.sql
│
├── Docs/
│   └── architecture.png
│
├── Sample_Data/
│   └── customer_data.json
│
└── README.md
```

---

# ⚠️ Challenges Faced

## 1. SCD Type 2 Implementation
Handling historical tracking and active record management.

## 2. JSON Parsing
Extracting nested JSON fields using VARIANT datatype.

## 3. Incremental Processing
Avoiding duplicate records during ingestion.

## 4. Task Dependency
Managing execution order between parent and child tasks.

## 5. Real-Time Automation
Ensuring automatic ingestion from AWS S3.

---

# 🚀 Future Improvements

- Add Error Logging Framework
- Implement Data Quality Checks
- Add Monitoring Dashboard
- Integrate Kafka Streaming
- Add CI/CD Deployment
- Implement Role-Based Security

---

# 📌 Key Learnings

- Snowflake Architecture
- Real-Time ETL Pipelines
- Data Warehousing Concepts
- SCD Type 2
- Stream Processing
- Pipeline Automation
- Cloud Data Engineering

---

# 👤 Author

<div align="center">

## Divyansh Vijay

![Made With](https://img.shields.io/badge/Made%20With-Snowflake%20%26%20SQL-29B5E8?style=for-the-badge)

### Data Engineer

Built a production-style cloud data engineering pipeline using Snowflake, AWS S3, Snowpipe, Streams, Tasks, and Medallion Architecture principles.

### Skills Used

- Snowflake
- SQL
- AWS S3
- ETL Pipelines
- Data Warehousing
- SCD Type 2
- Stream Processing