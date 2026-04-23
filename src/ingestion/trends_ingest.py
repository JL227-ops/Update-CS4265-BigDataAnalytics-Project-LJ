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


def load_google_trends(spark: SparkSession, path: str):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def main():
    config = load_config()
    spark = build_spark("M2_Google_Trends_Ingestion")

    trends_path = config["sources"]["google_trends"]
    trends_df = load_google_trends(spark, trends_path)

    print("\n===== Google Trends Sample =====")
    trends_df.printSchema()
    trends_df.show(10, truncate=False)
    print(f"Row count (sample action): {trends_df.limit(1000).count()}")

    spark.stop()


if __name__ == "__main__":
    main()
