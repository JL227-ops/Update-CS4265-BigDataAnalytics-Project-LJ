# Update-CS4265-BigDataAnalytics-Project-LJ
# Distributed Multi-Source Data Pipeline for Product Trend Analysis
A distributed big data analytics pipeline built with Apache Spark and Amazon S3 for analyzing large-scale electronics product trends using heterogeneous online data sources.

## Project Overview

This project integrates large-scale structured and unstructured datasets to analyze electronics product trends using distributed processing technologies.

The pipeline combines:

- Amazon Electronics Reviews in 2018
- Amazon Electronics Metadata in 2018
- Google Electronics Trends in 2018
- Common Crawl web text in 2018

The system performs distributed ingestion, cleaning, transformation, integration, aggregation, and validation to generate product-level analytical signals and trend insights.

The project demonstrates core Big Data concepts including:

- Distributed storage
- Distributed processing
- ETL pipeline design
- Schema normalization
- Multi-source integration
- Spark SQL analytics
- MapReduce-style transformations

---

# Problem Statement

Modern online platforms generate massive volumes of electronics product data from reviews, metadata, search trends, and web-scale text.

However:

- The datasets are too large for efficient single-machine processing
- The data sources use heterogeneous formats
- External signals do not share exact relational keys
- Large joins and aggregations create computational bottlenecks

This project addresses these challenges by building a scalable distributed pipeline capable of integrating multiple large-scale datasets for trend analysis.

---

# Big Data Characteristics

## Volume

- Amazon Reviews: 20,994,353 records
- Amazon Metadata: 786,445 records
- Common Crawl Sample: 7,006,623 records

## Variety

The pipeline integrates multiple heterogeneous formats:

- JSON
- CSV
- WET text
- Parquet

## Velocity

This project uses a batch-oriented processing model for large-scale analytical processing.

---

# Data Sources

| Dataset | Format | Description |
|---|---|---|
| Amazon Electronics Reviews | JSON | User reviews and ratings |
| Amazon Electronics Metadata | JSON | Product information and categories |
| Google Trends | CSV | Search interest over time |
| Common Crawl WET | Text | Large-scale web text signals |

---

# Technologies Used

| Technology | Purpose |
|---|---|
| Apache Spark | Distributed processing |
| Amazon S3 | Distributed object storage |
| Spark SQL | Distributed analytical queries |
| Python | Pipeline orchestration |
| Parquet | Columnar analytical storage |
| AWS CLI | S3 authentication and access |

---

## Design Rationale

Apache Spark was selected because the datasets exceed the practical processing limits of a single machine.
Amazon S3 provides scalable distributed object storage suitable for large-scale batch analytics pipelines.


# Pipeline Architecture

The distributed pipeline follows the architecture below:

S3 → Ingestion → Cleaning → Transformation → Integration → Aggregation → Output

Pipeline stages include:

1. Distributed ingestion from Amazon S3
2. Data cleaning and schema normalization
3. Transformation to Parquet format
4. Exact and approximate integration
5. Product-level aggregation
6. Validation and read-back verification
7. Queryable analytical outputs

---

# Integration Strategy

## Exact Integration

Amazon Reviews and Amazon Metadata are integrated using exact joins based on ASIN product identifiers.

## Approximate Integration

Google Trends and Common Crawl data do not share exact keys with Amazon data.

Approximate integration is performed using:

- Topic-level mapping
- Electronics category alignment
- Trend signal generation
- Web mention aggregation

---

# Repository Structure

```text
Update-CS4265-BigDataAnalytics-Project-LJ/
├── config/
│   ├── settings.yaml
│   └── .env.example
├── src/
│   ├── ingestion/
│   ├── preprocessing/
│   ├── integration/
│   ├── aggregation/
│   ├── storage/
│   └── main.py
├── docs/
│   ├── update_CS4265_JIA_LIU_M1.pdf
│   ├── update_CS4265_JIA_LIU_M2.pdf
│   └── evidence-M2/
│   │   └── amazon google trends successully.png
│   │   └── amazon review and metadata records.png
│   │   └── common crawl 10 records.png
│   │   └── data in S3.png
│   │   └── parquet.png
│   │   └── read back verification.png
│   └── evidence-M3/
│   │   └── starting pipeline run.png
│   │   └── Ingestion_Cleaning_Transformation.png
│   │   └── Integration and save data in S3.png
│   │   └── read back.png
│   │   └── sample query and pipeline complete.png
│   └── evidences-M4/
│   │   └── output sample-1.png
│   │   └── output sample-2.png
│   │   └── ingestion.png
│   │   └── clean and normalization.png
│   └── update_CS4265_JIA_LIU_M3.pdf
│   └── CS4265_JIA_LIU_M4.pdf
│   └── architecture.png
│   ├── architecture.md
│   ├── data_dictionary.md
│   └── validation.md
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md

```
---

# Installation

## Clone Repository

```bash
git clone https://github.com/JL227-ops/Update-CS4265-BigDataAnalytics-Project-LJ

cd Update-CS4265-BigDataAnalytics-Project-LJ
```

## Install Dependencies

```bash
pip install -r requirements.txt
```

## Configure AWS
```bash
aws configure
```

AWS credentials are required for full pipeline execution.
---

# Run Pipeline

```bash
spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.hadoop.fs.s3a.access.key=YOUR_KEY \
  --conf spark.hadoop.fs.s3a.secret.key=YOUR_SECRET \
  --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
  src/main.py
```
---

# Pipeline Processing Stages

## Ingestion

Load data from Amazon S3:

- Amazon Reviews
- Amazon Metadata
- Google Trends
- Common Crawl

## Cleaning & Transformation

- Remove invalid records
- Normalize fields
- Validate ratings
- Standardize categories
- Convert timestamps

## Integration

- Exact joins using ASIN
- Approximate topic-level integration
- External signal generation

## Aggregation

Generate product-level metrics:

- review_count
- avg_rating
- avg_review_length
- trend_score
- web_mention_count

## Output

Store integrated outputs as Parquet datasets in Amazon S3.

---

# Final Output Schema

| Field | Type | Description |
|---|---|---|
| asin | string | Product ID |
| title | string | Product title |
| brand | string | Product brand |
| review_count | long | Number of reviews |
| avg_rating | double | Average rating |
| avg_review_length | double | Average review length |
| latest_review_date | date | Most recent review |
| avg_trend_score | double | Google Trends signal |
| max_trend_score | integer | Maximum trend score |
| trend_record_count | long | Trend records |
| web_mention_count | long | Common Crawl mentions |
| avg_web_text_length | double | Web text length metric |

---

# Validation & Results

## Dataset Statistics

| Dataset | Records |
|---|---|
| Amazon Reviews | 20,994,353 |
| Amazon Metadata | 786,445 |
| Google Trends | 53 |
| Common Crawl | 7,006,623 |
| Integrated Reviews + Metadata | 21,835,272 |
| Product Signals | 756,420 |
| Final Output | 756,420 |

---

# Cleaning Results

| Dataset | Before | After | Removed |
|---|---|---|---|
| Amazon Reviews | 20,994,353 | 20,984,629 | 9,724 |
| Amazon Metadata | 786,445 | 786,445 | 0 |
| Google Trends | 53 | 53 | 0 |
| Common Crawl | 7,006,623 | 2,565,925 | 4,440,698 |

---

# Runtime Performance

| Pipeline Stage | Runtime |
|---|---|
| Ingestion | 269.99 seconds |
| Cleaning & Transformation | 494.14 seconds |
| Integration & Aggregation | 3014.17 seconds |
| Storage | 5313.79 seconds |
| Verification & Sample Query | 652.42 seconds |
| Total Runtime | 9763.16 seconds (~2.7 hours) |

---

# Data Quality Metrics

| Metric | Result |
|---|---|
| Invalid Amazon review records removed | 9,724 |
| Invalid Common Crawl records removed | 4,440,698 |
| Rating validation | 1–5 enforced |
| Schema conformance | Verified |
| Read-back verification | Successful |

---

# Sample Validation

The final output dataset was validated using Spark read-back verification and sample analytical queries.

Validation confirmed:

- Output schema correctness
- Successful distributed aggregation
- Correct preservation of product identifiers
- Proper integration of external trend signals
- Queryable Parquet outputs stored in S3

Example validated fields:

- asin
- title
- review_count
- avg_rating
- avg_trend_score
- web_mention_count

---

# Example Logging Output

```text
[INFO] Retrieved Amazon reviews: 20,994,353 records
[INFO] Retrieved Amazon metadata: 786,445 records
[INFO] Retrieved Google Trends: 53 records
[INFO] Retrieved Common Crawl sample: 7,006,623 records

[INFO] Ingestion stage completed in 269.99 seconds

[INFO] Cleaned Amazon reviews: 20,984,629 records
[INFO] Cleaned Common Crawl: 2,565,925 records

[INFO] Cleaning and transformation stage completed in 494.14 seconds

[INFO] Integrated Amazon reviews + metadata: 21,835,272 records

[INFO] Product-level signals: 756,420 records

[INFO] Final M3 output: 756,420 records

[INFO] Integration and aggregation stage completed in 3014.17 seconds

[INFO] Storage stage completed in 5313.79 seconds

[INFO] Verification and sample query stage completed in 652.42 seconds

[INFO] Pipeline complete. Duration: 9763.16 seconds
```

---
# Output

data sources and output stored in S3 
Final integrated dataset
Aggregated product-level signals
s3a://cs4265-bigdata-product-trends-jialiu/raw/google_trends/
  s3a://cs4265-bigdata-product-trends-jialiu/raw/metadata/
  s3a://cs4265-bigdata-product-trends-jialiu/raw/reviews/
  s3a://cs4265-bigdata-product-trends-jialiu/m3/clean/
  s3a://cs4265-bigdata-product-trends-jialiu/m3/clean/
  s3a://cs4265-bigdata-product-trends-jialiu/m3/integrated/
  s3a://cs4265-bigdata-product-trends-jialiu/m3/output/

# Edge Case Handling

| Edge Case | Handling |
|---|---|
| Invalid JSON records | Filtered during parsing |
| Missing fields | Removed during cleaning |
| Duplicate records | Removed using distributed filtering |
| Empty dataset | Aggregation safely skipped |
| Unexpected schema fields | Reduced schema applied |

---

# Known Limitations

- Common Crawl integration currently uses keyword-level matching
- Google Trends dataset is limited in granularity
- Large distributed joins remain computationally expensive
- Pipeline currently supports batch processing only
- Approximate matching may not fully capture semantic relationships

---

# Lessons Learned

Through this project, the following skills and concepts were developed:

- Distributed processing using Apache Spark
- Large-scale data storage using Amazon S3
- ETL pipeline design and orchestration
- Schema normalization across heterogeneous datasets
- Spark debugging and performance optimization
- Distributed joins and aggregation strategies
- Technical documentation using LaTeX and GitHub

---

# Future Improvements

Potential future improvements include:
- Improved semantic matching for external signals
- Better Spark partition optimization
- Additional trend-analysis features
- Real-time streaming support
- Advanced NLP processing for Common Crawl text

---

# Notes
AWS credentials are required for full pipeline execution.
Large-scale datasets are stored in Amazon S3 and are not included directly in the repository.

---

# License
MIT License
Copyright (c) 2026 Jia Liu

## Project Status

Milestone 4 complete.

The pipeline is fully functional and validated, with distributed ingestion, transformation, integration, aggregation, and verification successfully implemented.

## Author
Jia Liu
CS 4265 Big Data Analytics
Kennesaw State University
