import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR / "src"))

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from ingestion.amazon_ingest import load_amazon_reviews, load_amazon_metadata
from ingestion.trends_ingest import load_google_trends
from ingestion.commoncrawl_ingest import load_commoncrawl_sample
from storage.save_to_s3 import save_parquet, verify_parquet_readback

CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"
load_dotenv(ROOT_DIR / ".env")


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str) -> SparkSession:
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


def main():
    config = load_config()
    spark = build_spark("CS4265_M2_Full_Pipeline")

    print("\nLoading Amazon reviews...")
    reviews_df = load_amazon_reviews(spark, config["sources"]["amazon_reviews"])

    print("Loading Amazon metadata...")
    metadata_df = load_amazon_metadata(spark, config["sources"]["amazon_metadata"])

    print("Loading Google Trends...")
    trends_df = load_google_trends(spark, config["sources"]["google_trends"])

    print("Loading Common Crawl sample...")
    commoncrawl_df = load_commoncrawl_sample(spark, config["sources"]["common_crawl_sample"])

    print("\nSaving Parquet outputs...")
    save_parquet(reviews_df, config["outputs"]["reviews_parquet"], "Amazon reviews")
    save_parquet(metadata_df, config["outputs"]["metadata_parquet"], "Amazon metadata")
    save_parquet(trends_df, config["outputs"]["trends_parquet"], "Google Trends")
    save_parquet(commoncrawl_df, config["outputs"]["commoncrawl_parquet"], "Common Crawl sample")

    print("\nVerifying Parquet read-back...")
    verify_parquet_readback(spark, config["outputs"]["reviews_parquet"], "Amazon reviews")
    verify_parquet_readback(spark, config["outputs"]["metadata_parquet"], "Amazon metadata")
    verify_parquet_readback(spark, config["outputs"]["trends_parquet"], "Google Trends")
    verify_parquet_readback(spark, config["outputs"]["commoncrawl_parquet"], "Common Crawl sample")

    print("\nM2 proof-of-concept pipeline completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
