# System Architecture

## Distributed Multi-Source Data Pipeline for Product Trend Analysis

# 1. Overview

This project implements a distributed multi-source data pipeline for electronics product trend analysis using Apache Spark and Amazon S3.

The system integrates large-scale structured and unstructured datasets from multiple online sources to generate product-level analytical signals.

The architecture shows several Big Data concepts as below:

- distributed storage
- distributed processing
- ETL pipeline design
- schema normalization
- distributed aggregation
- scalable analytical querying

---

# 2. High-Level Architecture

The pipeline follows a distributed batch-processing architecture.

```text
Amazon S3
   ↓
Distributed Ingestion
   ↓
Cleaning & Transformation
   ↓
Schema Normalization
   ↓
Multi-Source Integration
   ↓
Distributed Aggregation
   ↓
Parquet Output Generation
   ↓
Validation & Querying
```
---

# 3. Core Technologies

| Technology | Purpose |
|---|---|
| Apache Spark | Distributed processing |
| Amazon S3 | Distributed object storage |
| Spark SQL | Distributed analytical querying |
| Python | Pipeline orchestration |
| Parquet | Columnar analytical storage |
| AWS CLI | S3 authentication |

---

# 4. Distributed Storage Layer

Amazon S3 serves as the distributed storage layer.

The system stores:

- raw JSON review datasets
- metadata datasets
- Google Trends CSV data
- Common Crawl WET files
- cleaned datasets
- integrated outputs
- final Parquet outputs

S3 provides:

- scalable object storage
- distributed persistence
- fault tolerance
- integration with Spark

---

# 5. Distributed Processing Layer

Apache Spark performs distributed processing across all pipeline stages.

Spark operations include:

- distributed ingestion
- distributed filtering
- schema normalization
- distributed joins
- distributed aggregation
- analytical querying

Spark DataFrames and Spark SQL were used for scalable structured processing.

---

# 6. ETL Pipeline Design

The project implements a complete ETL workflow.

## Extract

Distributed ingestion from Amazon S3.

### Extracted datasets

- Amazon Reviews
- Amazon Metadata
- Google Trends
- Common Crawl

---

## Transform

Transformation stage includes:

- cleaning invalid records
- schema normalization
- timestamp conversion
- distributed joins
- external signal integration
- analytical aggregation

---

## Load

The final analytical datasets are stored as Parquet outputs in Amazon S3.

---

# 7. Integration Strategy

## Exact Integration

Amazon Reviews and Metadata datasets were integrated using exact joins on ASIN product identifiers.

---

## Approximate Integration

Google Trends and Common Crawl datasets do not contain direct relational keys.

Approximate integration was performed using:

- electronics topic mapping
- category alignment
- trend signal aggregation
- web mention extraction

---

# 8. Aggregation Strategy

The final pipeline generates product-level analytical outputs.

Generated metrics include:

- review_count
- avg_rating
- avg_review_length
- avg_trend_score
- web_mention_count

Product-level aggregation improves:

- scalability
- analytical query performance
- storage efficiency

---

# 9. Distributed Processing Concepts Applied

The implementation applies several distributed system concepts.

## MapReduce-Style Processing

Spark transformations follow the MapReduce processing model.

Examples include:

- distributed map-style extraction
- shuffle-based joins
- distributed reduce aggregations

---

## Parallel Execution

Spark partitions datasets and executes transformations in parallel.

Parallel processing improves scalability for large-scale datasets.

---

## Schema Alignment

Schema normalization was applied to align heterogeneous datasets into a unified analytical structure.

---
