from pathlib import Path
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

ROOT_DIR = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT_DIR / "config" / "settings.yaml"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.endpoint', 's3.amazonaws.com')
        .config('spark.sql.session.timeZone', 'America/New_York')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    return spark


def review_schema() -> StructType:
    return StructType([
        StructField('asin', StringType(), True),
        StructField('reviewerID', StringType(), True),
        StructField('reviewerName', StringType(), True),
        StructField('overall', DoubleType(), True),
        StructField('reviewText', StringType(), True),
        StructField('summary', StringType(), True),
        StructField('unixReviewTime', LongType(), True),
        StructField('reviewTime', StringType(), True),
        StructField('verified', StringType(), True),
        StructField('vote', StringType(), True),
    ])


def metadata_schema() -> StructType:
    return StructType([
        StructField('asin', StringType(), True),
        StructField('title', StringType(), True),
        StructField('brand', StringType(), True),
        StructField('price', StringType(), True),
        StructField('description', ArrayType(StringType()), True),
        StructField('categories', ArrayType(ArrayType(StringType())), True),
        StructField('main_cat', StringType(), True),
    ])


def load_json_lines_subset(spark: SparkSession, path: str, schema: StructType):
    raw_df = spark.read.text(path)
    parsed_df = raw_df.select(from_json(col('value'), schema).alias('j'))
    return parsed_df.select('j.*')

def load_amazon_reviews(spark: SparkSession, path: str):
    df = load_json_lines_subset(spark, path, review_schema())
    return df.filter(col('asin').isNotNull())


def load_amazon_metadata(spark: SparkSession, path: str):
    df = load_json_lines_subset(spark, path, metadata_schema())
    return df.filter(col('asin').isNotNull())


def preview_dataframe(df, title: str, limit: int = 10):
    print(f'\n===== {title} =====')
    df.printSchema()
    df.show(limit, truncate=False)
    print(f'Row count (sample action): {df.limit(1000).count()}')


def main():
    config = load_config()
    spark = build_spark('M2_Amazon_Ingestion_Safe')

    reviews_path = config['sources']['amazon_reviews']
    metadata_path = config['sources']['amazon_metadata']

    reviews_df = load_json_lines_subset(spark, reviews_path, review_schema())
    metadata_df = load_json_lines_subset(spark, metadata_path, metadata_schema())

    reviews_df = reviews_df.filter(col('asin').isNotNull())
    metadata_df = metadata_df.filter(col('asin').isNotNull())

    preview_dataframe(reviews_df, 'Amazon Reviews Sample (Reduced Schema)')
    preview_dataframe(metadata_df, 'Amazon Metadata Sample (Reduced Schema)')

    spark.stop()


if __name__ == '__main__':
    main()
