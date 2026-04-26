
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, avg, max as spark_max, lit


def join_reviews_metadata(reviews_df: DataFrame, metadata_df: DataFrame) -> DataFrame:
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
    trend_summary = (
        trends_df
        .groupBy("trend_year", "trend_month")
        .agg(avg("trend_score").alias("avg_trend_score"))
    )

    # M3 simple integration: add general electronics trend signal.
    # More detailed category/keyword matching can be expanded in M4.
    product_with_signal = (
        product_df
        .withColumn("integration_topic", lit("electronics"))
    )

    return product_with_signal
