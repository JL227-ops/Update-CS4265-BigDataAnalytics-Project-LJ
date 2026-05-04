
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    avg,
    max as spark_max,
    lit,
    length,
)


def join_reviews_metadata(reviews_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
    """
    Join Amazon reviews with Amazon metadata using ASIN.
    This is the exact join between the two Amazon data sources.
    """
    return (
        reviews_df
        .join(metadata_df, on="asin", how="left")
        .select(
            "asin",
            "reviewerID",
            "overall",
            "reviewText",
            "review_text_clean",
            "review_date",
            "review_year",
            "review_month",
            "review_length",
            "title",
            "brand",
            "price",
            "price_clean",
            "categories"
        )
    )


def aggregate_product_signals(joined_df: DataFrame) -> DataFrame:
    """
    Aggregate review-level data into product-level signals.
    This creates meaningful analytical output from Amazon review data.
    """
    return (
        joined_df
        .groupBy("asin", "title", "brand")
        .agg(
            count("*").alias("review_count"),
            avg("overall").alias("avg_rating"),
            avg("review_length").alias("avg_review_length"),
            spark_max("review_date").alias("latest_review_date")
        )
        .filter(col("review_count") >= 1)
        .orderBy(col("review_count").desc())
    )


def integrate_with_trends(product_df: DataFrame, trends_df: DataFrame) -> DataFrame:
    """
    Integrate product-level Amazon signals with Google Trends.

    Google Trends does not share ASIN with Amazon data, so this project uses
    topic-level integration. The cleaned Trends data is aggregated into overall
    trend signals and joined into the product-level output using integration_topic.
    """
    trend_summary = (
        trends_df
        .groupBy()
        .agg(
            avg("trend_score").alias("avg_trend_score"),
            spark_max("trend_score").alias("max_trend_score"),
            count("*").alias("trend_record_count")
        )
        .withColumn("integration_topic", lit("electronics"))
    )

    product_with_topic = (
        product_df
        .withColumn("integration_topic", lit("electronics"))
    )

    return (
        product_with_topic
        .join(trend_summary, on="integration_topic", how="left")
    )


def aggregate_commoncrawl_signals(commoncrawl_df: DataFrame) -> DataFrame:
    """
    Convert cleaned Common Crawl text into web-scale mention signals.

    Common Crawl is unstructured text, so it is integrated as an external
    web attention signal. This function calculates web mention count and
    average text length, then joins those signals using integration_topic.
    """
    return (
        commoncrawl_df
        .withColumn("integration_topic", lit("electronics"))
        .groupBy("integration_topic")
        .agg(
            count("*").alias("web_mention_count"),
            avg(length(col("text_clean"))).alias("avg_web_text_length")
        )
    )


def integrate_with_commoncrawl(
    product_trend_df: DataFrame,
    commoncrawl_signals_df: DataFrame
) -> DataFrame:
    """
    Join Common Crawl web signals into the final product trend dataset.
    """
    return (
        product_trend_df
        .join(commoncrawl_signals_df, on="integration_topic", how="left")
    )
