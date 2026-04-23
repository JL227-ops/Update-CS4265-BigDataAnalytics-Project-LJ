import os
import sys
from pathlib import Path

import yaml
from dotenv import load_dotenv
from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parents[2]
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
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_commoncrawl_sample(spark: SparkSession, path: str):
    return spark.read.text(path)


def main():
    config = load_config()
    spark = build_spark("M2_CommonCrawl_Sample_Ingestion")

    cc_path = config["sources"]["common_crawl_sample"]
    cc_df = load_commoncrawl_sample(spark, cc_path)

    print("\n===== Common Crawl WET Sample =====")
    cc_df.printSchema()
    cc_df.show(10, truncate=False)
    print(f"Line count (sample action): {cc_df.limit(1000).count()}")

    spark.stop()


if __name__ == "__main__":
    main()
