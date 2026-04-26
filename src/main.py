import sys
import time
import logging
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR / "src"))

import yaml
from pyspark.sql import SparkSession

from ingestion.amazon_ingest import load_amazon_reviews, load_amazon_metadata
from ingestion.trends_ingest import load_google_trends
from ingestion.commoncrawl_ingest import load_commoncrawl_sample
from storage.save_to_s3 import save_parquet, verify_parquet_readback

from preprocessing.clean_transform import (
    clean_reviews,
    clean_metadata,
    clean_trends,
    clean_commoncrawl,
)

from integration.integrate_sources import (
    join_reviews_metadata,
    aggregate_product_signals,
    integrate_with_trends,
)

CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"


logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CS4265_M3")


def load_config():
    """Load pipeline configuration from settings.yaml."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str) -> SparkSession:
    """Create a Spark session configured for S3-based distributed processing."""
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def require_config(config, section, key):
    """Validate required configuration values."""
    if section not in config or key not in config[section]:
        raise ValueError(f"Missing config value: {section}.{key}")
    return config[section][key]


def log_count(label, df):
    """Count a DataFrame and log the number of records."""
    count = df.count()
    logger.info(f"{label}: {count:,} records")
    return count


def log_cleaning_result(name, before_count, after_count):
    """Log before/after cleaning counts and removed percentage."""
    removed = before_count - after_count
    removed_pct = (removed / before_count * 100) if before_count else 0
    logger.info(
    f"Cleaned {name}: {after_count:,} records "
    f"({removed:,} removed, {removed_pct:.2f}% removed)"
)


def log_stage_duration(stage_name, stage_start):
    """Log how long a pipeline stage took."""
    duration = round(time.time() - stage_start, 2)
    logger.info("%s completed in %.2f seconds", stage_name, duration)


def save_with_logging(df, path, label):
    """Write a DataFrame to S3 as Parquet with clear logging."""
    logger.info("Writing %s to S3: %s", label, path)
    save_parquet(df, path, label)
    logger.info("Finished writing %s", label)


def main():
    """Run the complete M3 pipeline from ingestion to verified final output."""
    pipeline_start = time.time()
    logger.info("Starting pipeline run")

    config = load_config()
    spark = build_spark("CS4265_M3_Complete_Pipeline")

    try:
        reviews_path = require_config(config, "sources", "amazon_reviews")
        metadata_path = require_config(config, "sources", "amazon_metadata")
        trends_path = require_config(config, "sources", "google_trends")
        commoncrawl_path = require_config(config, "sources", "common_crawl_sample")

        # 1. Ingestion
        stage_start = time.time()
        logger.info("Fetching Amazon reviews from %s", reviews_path)
        reviews_raw = load_amazon_reviews(spark, reviews_path)
        reviews_raw_count = log_count("Retrieved Amazon reviews", reviews_raw)

        logger.info("Fetching Amazon metadata from %s", metadata_path)
        metadata_raw = load_amazon_metadata(spark, metadata_path)
        metadata_raw_count = log_count("Retrieved Amazon metadata", metadata_raw)

        logger.info("Fetching Google Trends from %s", trends_path)
        trends_raw = load_google_trends(spark, trends_path)
        trends_raw_count = log_count("Retrieved Google Trends", trends_raw)

        logger.info("Fetching Common Crawl sample from %s", commoncrawl_path)
        commoncrawl_raw = load_commoncrawl_sample(spark, commoncrawl_path)
        commoncrawl_raw_count = log_count("Retrieved Common Crawl sample", commoncrawl_raw)

        log_stage_duration("Ingestion stage", stage_start)

        # 2. Cleaning / Transformation
        stage_start = time.time()
        logger.info("Applying cleaning and transformations to Amazon reviews")
        reviews_clean = clean_reviews(reviews_raw)
        reviews_clean_count = log_count("Transformed Amazon reviews", reviews_clean)
        log_cleaning_result("Amazon reviews", reviews_raw_count, reviews_clean_count)

        logger.info("Applying cleaning and transformations to Amazon metadata")
        metadata_clean = clean_metadata(metadata_raw)
        metadata_clean_count = log_count("Transformed Amazon metadata", metadata_clean)
        log_cleaning_result("Amazon metadata", metadata_raw_count, metadata_clean_count)

        logger.info("Applying cleaning and transformations to Google Trends")
        trends_clean = clean_trends(trends_raw)
        trends_clean_count = log_count("Transformed Google Trends", trends_clean)
        log_cleaning_result("Google Trends", trends_raw_count, trends_clean_count)

        logger.info("Applying cleaning and transformations to Common Crawl")
        commoncrawl_clean = clean_commoncrawl(commoncrawl_raw)
        commoncrawl_clean_count = log_count("Transformed Common Crawl", commoncrawl_clean)
        log_cleaning_result("Common Crawl", commoncrawl_raw_count, commoncrawl_clean_count)

        log_stage_duration("Cleaning and transformation stage", stage_start)

        # 3. Integration / Aggregation
        stage_start = time.time()
        logger.info("Integrating Amazon reviews with metadata")
        amazon_integrated = join_reviews_metadata(reviews_clean, metadata_clean)
        amazon_integrated_count = log_count(
            "Integrated Amazon reviews + metadata",
            amazon_integrated,
        )

        logger.info("Creating product-level aggregate signals")
        product_signals = aggregate_product_signals(amazon_integrated)
        product_signals_count = log_count("Product-level signals", product_signals)

        logger.info("Integrating product signals with Google Trends")
        final_output = integrate_with_trends(product_signals, trends_clean)
        final_output_count = log_count("Final M3 output", final_output)

        log_stage_duration("Integration and aggregation stage", stage_start)

        # 4. Storage
        stage_start = time.time()
        logger.info("Writing cleaned, integrated, and analytical outputs to S3")

        save_with_logging(
            reviews_clean,
            config["outputs"]["reviews_clean_parquet"],
            "Clean Amazon reviews",
        )
        save_with_logging(
            metadata_clean,
            config["outputs"]["metadata_clean_parquet"],
            "Clean Amazon metadata",
        )
        save_with_logging(
            trends_clean,
            config["outputs"]["trends_clean_parquet"],
            "Clean Google Trends",
        )
        save_with_logging(
            commoncrawl_clean,
            config["outputs"]["commoncrawl_clean_parquet"],
            "Clean Common Crawl",
        )
        save_with_logging(
            amazon_integrated,
            config["outputs"]["amazon_integrated_parquet"],
            "Integrated Amazon reviews metadata",
        )
        save_with_logging(
            product_signals,
            config["outputs"]["product_signals_parquet"],
            "Product-level signals",
        )
        save_with_logging(
            final_output,
            config["outputs"]["final_output_parquet"],
            "Final M3 output",
        )

        log_stage_duration("Storage stage", stage_start)

        # 5. Verification / Testing
        stage_start = time.time()
        logger.info("Verifying final output read-back")
        verify_parquet_readback(
            spark,
            config["outputs"]["final_output_parquet"],
            "Final M3 output",
        )

        logger.info("Running sample query on final output")
        final_output.select(
            "asin",
            "title",
            "brand",
            "review_count",
            "avg_rating",
            "avg_review_length",
        ).show(10, truncate=False)

        log_stage_duration("Verification and sample query stage", stage_start)

        total_duration = round(time.time() - pipeline_start, 2)
        logger.info("Pipeline complete. Duration: %.2f seconds", total_duration)
        logger.info(f"Final output contains {final_output_count:,} records")
        logger.info(
            "M3 summary: reviews=%s, metadata=%s, trends=%s, commoncrawl=%s, integrated=%s, product_signals=%s, final=%s",
            f"{reviews_raw_count:,}",
            f"{metadata_raw_count:,}",
            f"{trends_raw_count:,}",
            f"{commoncrawl_raw_count:,}",
            f"{amazon_integrated_count:,}",
            f"{product_signals_count:,}",
            f"{final_output_count:,}",
        )

    except Exception as e:
        logger.exception("M3 pipeline failed: %s", e)
        raise

    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
