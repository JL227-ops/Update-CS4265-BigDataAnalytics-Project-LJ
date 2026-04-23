# Update-CS4265-BigDataAnalytics-Project-LJ
# Distributed Multi-Source Data Pipeline for Product Trend Analysis

## M1-Project Overview

This project builds a distributed Big Data pipeline to identify emerging product trends and analyze consumer interests by integrating multiple heterogeneous data sources.

## Data Sources

* Amazon Electronics Reviews and Metadata (2018, JSON)
* Common Crawl Web Text Data (2018, WET)
* Google Trends Data (2018, CSV)

These datasets provide complementary signals:

* Product interactions (Amazon)
* Web-scale public signals (Common Crawl)
* External trend indicators (Google Trends)

## System Design

The system follows a distributed pipeline architecture:

* Data ingestion into Amazon S3
* Data cleaning and normalization
* Feature extraction (keywords, categories, time signals)
* Distributed joins and approximate matching
* Aggregation for trend detection
* Query and analytics using Spark SQL

## Project Structure

```bash id="i7hck7"
update_CS4265_Project_Jia_Liu/
|
|-- data/
|   |-- raw/              # Amazon, Common Crawl, Google Trends
|   |-- processed/
|   `-- outputs/
|
|-- src/
|   |-- ingestion/
|   |-- preprocessing/
|   |-- integration/               # Distributed joins
|   `-- aggregation/               # Trend analysis
|
|-- notebooks/
|
|-- docs/
|   `-- update_CS4265_JIA_LIU_M1/  # M1 Proposal
|
|-- config/
|   |-- settings.yaml              # Pipeline configuration
|   `-- env.example                # Environment variables
|
|-- requirements.txt
`-- .gitignore
```

## Technology Stack

* Storage: Amazon S3
* Processing: Apache Spark
* Data Model: Spark DataFrames / RDDs
* Query: Spark SQL
* Formats: JSON, WET, CSV to Parquet

## How to Run

```bash id="tyg3mp"
pip install -r requirements.txt

python src/ingestion/load_data.py
python src/preprocessing/clean_data.py
python src/integration/join_data.py
python src/aggregation/analyze_trends.py
```

## Outputs

* Emerging product categories
* Cross-source trend validation
* Consumer interest insights

## Future Work

* Machine learning for trend prediction
* Visualization dashboards
* Real-time streaming pipeline

## M2 - Initial Implementation

This project builds a distributed multi-source data pipeline for product trend analysis by integrating three heterogeneous data sources:

1. **Amazon Electronics Reviews & Metadata** (JSON / JSON.GZ)  
2. **Common Crawl WET files** (web-scale text data)  
3. **Google Trends data** (CSV)  

The goal of Milestone 2 is to demonstrate that the project is a **working proof-of-concept**, showing that:

- data acquisition works,
- data can be stored persistently,
- the pipeline has a clear and modular structure.

This repository focuses on the **working plumbing** for M2 rather than a full analytical system.

---

## Current M2 Status

### What is working
- Amazon reviews ingestion from S3 with Spark  
- Amazon metadata ingestion from S3 with Spark  
- Google Trends CSV ingestion from S3 with Spark  
- Common Crawl sample ingestion from S3 with Spark  
- Persistent storage to S3 in **Parquet format (columnar storage)**  
- Read-back verification from stored Parquet files (schema + sample + row count)  
- Modular project structure with orchestration script  

### Still in progress for M3
- Data cleaning and normalization across all sources  
- Category/keyword-based integration across sources  
- Trend aggregation and analytics  
- Full large-scale Common Crawl processing  
- Optional graph-based extensions for relationship discovery  

---

## Repository Structure

```text
CS4265_M2_Package_Cindy_Liu/
├── config/
│   ├── settings.yaml
│   └── .env.example
├── src/
│   ├── ingestion/
│   │   ├── amazon_ingest.py
│   │   ├── commoncrawl_ingest.py
│   │   └── trends_ingest.py
│   ├── storage/
│   │   └── save_to_s3.py
│   ├── processing/
│   │   └── placeholder.py
│   └── main.py
├── data/
│   ├── raw/
│   └── processed/
├── docs/
│   ├── M2_Progress_Report.docx
│   └── M2_Progress_Report.pdf
├── logs/
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Technology Choices
- **Storage:** Amazon S3
- **Processing:** Apache Spark / PySpark
- **Formats:** JSON, CSV, Parquet, WET text
- **Configuration:** YAML + environment variables

---
##Distributed Processing

This project uses Apache Spark to perform distributed processing. Data is automatically partitioned and processed in parallel when reading from S3, applying transformations, and writing Parquet outputs.
Even though the current implementation runs in local mode, Spark still executes operations across partitions, demonstrating a scalable pipeline design that can be extended to a full cluster environment.

## Environment Setup

Create and activate a Python environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Credentials
This project expects AWS credentials to be configured through one of the following:
- AWS CLI (`aws configure`)
- environment variables
- IAM role (if running on AWS)
---
Required environment variables (example):
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION

## Configuration
Edit `config/settings.yaml` to match your bucket and paths.
You can also define credentials in a local `.env` file based on `.env.example`.

---

## How to Run
Run the full M2 proof-of-concept pipeline:
```bash
spark-submit src/main.py
```

Or run individual stages:

```bash
spark-submit --packages org.apache.hadoop:hadoop-aws:3.4.2 \
--conf spark.hadoop.fs.s3a.access.key=key \
--conf spark.hadoop.fs.s3a.secret.key=secret \
--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
src/main.py
```

---

## Expected M2 Outputs
The pipeline writes Parquet outputs to the `processed/` area in S3:

- `processed/reviews_parquet/`
- `processed/metadata_parquet/`
- `processed/trends_parquet/`
- `processed/commoncrawl_sample_parquet/`
Each dataset is written in partitioned Parquet format.
The pipeline also reads the stored Parquet data back to verify correctness.

---

## Evidence for Milestone 2
For submission, include:
- screenshots of Spark DataFrame outputs (show(10))
- schema outputs confirming structured data
- sample row counts (e.g., 1000 records for ingestion verification)
- S3 directory listing showing Parquet files
- successful pipeline execution logs (main.py)
- repository URL
- M2 progress report PDF
- M2 progress report PDF.

---

## Notes on Scope
This Milestone 2 implementation intentionally focuses on **viability** rather than full analytics. The larger goal for M3 is to clean, normalize, integrate, and analyze these datasets in a distributed environment.


## Author

Jia Liu
CS 4265 Big Data Analytics
Kennesaw State University
