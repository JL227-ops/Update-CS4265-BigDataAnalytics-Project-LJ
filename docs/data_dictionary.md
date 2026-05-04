# Data Dictionary

## Distributed Multi-Source Data Pipeline for Product Trend Analysis
---

# 1. Overview

This document describes the datasets, schemas, and analytical fields used in the distributed multi-source product trend analysis pipeline.

The pipeline integrates multiple heterogeneous datasets including:

- Amazon Electronics Reviews in 2018
- Amazon Electronics Metadata in 2018
- Google Electronics Trends in 2018
- Common Crawl web text in 2018

The final analytical output is structured at the product level and stored in Parquet format in Amazon S3.

---

# 2. Source Datasets

## 2.1 Amazon Electronics Reviews

### Format
JSON

### Description
Contains user review information for electronics products.

### Key Fields

| Field | Type | Description |
|---|---|---|
| asin | string | Amazon product identifier |
| overall | double | User rating score |
| reviewText | string | Review content |
| summary | string | Review title |
| unixReviewTime | long | Review timestamp |
| reviewerID | string | Reviewer identifier |

### Purpose

Used to generate:

- review counts
- average ratings
- review length metrics
- review activity signals

---

## 2.2 Amazon Electronics Metadata

### Format
JSON

### Description
Contains electronics product metadata and category information.

### Key Fields

| Field | Type | Description |
|---|---|---|
| asin | string | Amazon product identifier |
| title | string | Product title |
| brand | string | Product brand |
| category | array/string | Product category |
| price | double | Product price |

### Purpose

Used to enrich review data with:

- product titles
- brand information
- category alignment

---

## 2.3 Google Electronics Trends Dataset

### Format
CSV

### Description
Contains search trend signals related to electronics topics.

### Key Fields

| Field | Type | Description |
|---|---|---|
| topic | string | Electronics topic |
| trend_score | integer | Relative trend interest |
| date | date/string | Trend date |

### Purpose

Used to generate:

- trend signals
- trend aggregation metrics
- external popularity indicators

---

## 2.4 Common Crawl Dataset

### Format
WET/Text

### Description
Large-scale web text dataset used for extracting web attention signals.

### Key Fields

| Field | Type | Description |
|---|---|---|
| raw_text | string | Extracted web text |
| crawl_date | string | Crawl timestamp |
| source_url | string | Source web URL |

### Purpose

Used to generate:

- web mention counts
- web attention metrics
- external trend signals

---

# 3. Final Integrated Output Schema

The final analytical dataset is aggregated at the product level.

---

## Final Output Fields

| Field | Type | Description |
|---|---|---|
| integration_topic | string | Electronics topic category |
| asin | string | Amazon product identifier |
| title | string | Product title |
| brand | string | Product brand |
| review_count | long | Total number of reviews |
| avg_rating | double | Average product rating |
| avg_review_length | double | Average review text length |
| latest_review_date | date | Most recent review date |
| avg_trend_score | double | Average Google Trends score |
| max_trend_score | integer | Maximum trend score |
| trend_record_count | long | Number of trend records |
| web_mention_count | long | Number of Common Crawl mentions |
| avg_web_text_length | double | Average web text length |

---

# 4. Data Cleaning and Validation

Several preprocessing and validation procedures were applied.

## Cleaning Procedures

- Removed invalid review records
- Filtered malformed JSON entries
- Standardized timestamps
- Reduced inconsistent nested schemas
- Removed duplicate records
- Filtered invalid ratings outside range 1–5

---

# 5. Storage Format

## Raw Data

Stored in Amazon S3 as:

- JSON
- CSV
- WET text

## Processed Data

Stored as:

- Parquet datasets

---

# 6. Data Aggregation Strategy

The final analytical outputs are aggregated at the product level.

## Reasons for Product-Level Aggregation

- Reduces data size
- Improves analytical query performance
- Simplifies trend analysis
- Supports scalable distributed analytics

---

# 7. Data Integration Strategy

## Exact Integration

Amazon Reviews and Metadata datasets were integrated using exact joins on ASIN product identifiers.

## Approximate Integration

Google Trends and Common Crawl datasets were integrated using:

- electronics topic mapping
- category alignment
- external signal aggregation

---

# 8. Validation Summary

Validation procedures confirmed:

- schema consistency
- successful distributed aggregation
- readable Parquet outputs
- queryable analytical datasets
- correct preservation of product identifiers