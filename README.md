# Update-CS4265-BigDataAnalytics-Project-LJ
# Distributed Multi-Source Data Pipeline for Product Trend Analysis

## Project Overview

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

## Author

Jia Liu
CS 4265 Big Data Analytics
Kennesaw State University
