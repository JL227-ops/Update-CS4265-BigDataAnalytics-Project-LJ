# Validation Report

## Distributed Multi-Source Data Pipeline for Product Trend Analysis

Author: Jia Liu  
Course: CS4265 Big Data Analytics  
Institution: Kennesaw State University  
Semester: Spring 2026

---

# 1. Validation Overview

This document summarizes the validation procedures, data quality metrics, sample validations, edge case handling, and performance results for the distributed multi-source data pipeline.

The validation stage was designed to verify:

- correctness of distributed transformations
- schema consistency
- successful multi-source integration
- data quality improvements
- queryable analytical outputs
- runtime performance

Validation was performed using Apache Spark read-back verification, sample analytical queries, runtime logging, and record count comparisons.

---

# 2. Dataset Statistics

The final pipeline successfully processed the following datasets.

| Dataset | Records |
|---|---|
| Amazon Reviews | 20,994,353 |
| Amazon Metadata | 786,445 |
| Google Trends | 53 |
| Common Crawl Sample | 7,006,623 |
| Integrated Reviews + Metadata | 21,835,272 |
| Product Signals | 756,420 |
| Final Output | 756,420 |

---

# 3. Data Quality Metrics

Several data quality checks and cleaning procedures were applied during preprocessing.

## Cleaning Metrics

| Dataset | Before | After | Removed |
|---|---|---|---|
| Amazon Reviews | 20,994,353 | 20,984,629 | 9,724 |
| Amazon Metadata | 786,445 | 786,445 | 0 |
| Google Trends | 53 | 53 | 0 |
| Common Crawl | 7,006,623 | 2,565,925 | 4,440,698 |

---

## Validation Metrics

| Metric | Result |
|---|---|
| Invalid Amazon review records removed | 9,724 |
| Invalid Common Crawl records removed | 4,440,698 |
| Rating validation | Ratings constrained to range 1–5 |
| Schema conformance | Verified |
| Duplicate handling | Applied during preprocessing |
| Read-back verification | Successful |
| Final output queryability | Verified |

---

# 4. Schema Validation

The final analytical dataset schema was validated using Spark read-back verification.

Validated output fields include:

| Field | Type |
|---|---|
| integration_topic | string |
| asin | string |
| title | string |
| brand | string |
| review_count | long |
| avg_rating | double |
| avg_review_length | double |
| latest_review_date | date |
| avg_trend_score | double |
| max_trend_score | integer |
| trend_record_count | long |
| web_mention_count | long |
| avg_web_text_length | double |

Schema validation confirmed that distributed transformations preserved field consistency and analytical structure after integration and aggregation.

---

# 5. Sample Validation

Sample validation was performed using Spark analytical queries on the final integrated output.

The validation stage confirmed:

- correct preservation of product identifiers
- successful integration of metadata
- accurate distributed aggregations
- successful external signal integration
- queryable Parquet outputs stored in S3

---

## Example Validation Output

### Example Product Record

| Field | Example Value |
|---|---|
| asin | 0985262788 |
| title | Bluetooth Workout Headphones |
| brand | Arena Club |
| review_count | 12 |
| avg_rating | 5.0 |
| avg_review_length | 361.42 |

---

## Validation Procedure

The final output was verified by:

1. Reading final Parquet outputs from Amazon S3
2. Running Spark schema verification
3. Executing distributed sample queries
4. Comparing aggregated outputs with expected analytical structure

The validation confirmed that distributed aggregations preserved analytical correctness after integration and transformation.

---

# 6. Read-Back Verification

Read-back verification was performed on the final Parquet outputs stored in Amazon S3.

The validation process confirmed:

- successful output persistence
- readable Parquet datasets
- valid Spark schema reconstruction
- consistent analytical outputs
- queryable distributed storage

The final output dataset was successfully loaded back into Spark for validation queries.

---

# 7. Edge Case Handling

The pipeline includes handling for several edge cases encountered during distributed processing.

| Edge Case | Handling Strategy |
|---|---|
| Invalid JSON records | Filtered during parsing |
| Missing fields | Removed during preprocessing |
| Duplicate records | Removed using distributed filtering |
| Unexpected schema fields | Reduced schema applied |
| Empty datasets | Aggregation safely skipped |
| Invalid ratings | Removed outside range 1–5 |
| Large-scale text noise | Filtered during Common Crawl cleaning |

These procedures improved stability and reliability during distributed execution.

---

# 8. Runtime Performance Results

The final distributed pipeline completed successfully in approximately 2.7 hours.

## Pipeline Runtime

| Pipeline Stage | Runtime |
|---|---|
| Ingestion | 269.99 seconds |
| Cleaning & Transformation | 494.14 seconds |
| Integration & Aggregation | 3014.17 seconds |
| Storage | 5313.79 seconds |
| Verification & Sample Query | 652.42 seconds |
| Total Runtime | 9763.16 seconds |

---

# 9. Distributed Processing Validation

The project successfully demonstrated several distributed processing concepts.

Validated distributed processing behaviors include:

- distributed ingestion from Amazon S3
- Spark partition-based parallel execution
- distributed joins and aggregations
- scalable schema normalization
- distributed analytical querying
- distributed Parquet output generation

The pipeline successfully processed large-scale datasets that exceed the practical limits of single-machine analytical processing.

---

# 10. Known Limitations

Although the pipeline successfully demonstrates distributed multi-source integration, several limitations remain.

- Common Crawl integration currently uses keyword-level matching
- Google Trends data granularity is limited
- Large distributed joins remain computationally expensive
- The current implementation supports batch processing only
- Approximate integration may not fully capture semantic relationships

---

# 11. Validation Summary

The validation procedures confirmed that the distributed pipeline successfully:
- ingested large-scale heterogeneous datasets
- cleaned and normalized inconsistent data
- integrated multiple distributed data sources
- generated correct product-level analytical outputs
- preserved schema consistency
- produced queryable Parquet datasets
- executed successfully using distributed Spark processing

The final implementation is fully functional, validated, and suitable for scalable electronics product trend analysis.
