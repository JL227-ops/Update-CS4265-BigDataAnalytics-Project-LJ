from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lower, trim, regexp_replace, to_date, from_unixtime,
    year, month, length, when
)


def clean_reviews(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("asin").isNotNull())
        .filter(col("reviewerID").isNotNull())
        .filter(col("overall").isNotNull())
        .filter(col("overall").between(1, 5))
        .withColumn("reviewText", trim(col("reviewText")))
        .filter(col("reviewText").isNotNull())
        .filter(length(col("reviewText")) > 0)
        .withColumn("review_date", to_date(from_unixtime(col("unixReviewTime"))))
        .withColumn("review_year", year(col("review_date")))
        .withColumn("review_month", month(col("review_date")))
        .withColumn("review_text_clean", lower(regexp_replace(col("reviewText"), "[^a-zA-Z0-9\\s]", " ")))
        .withColumn("review_length", length(col("reviewText")))
    )


def clean_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("asin").isNotNull())
        .withColumn("title", trim(col("title")))
        .withColumn("brand", trim(col("brand")))
        .withColumn(
            "brand",
            when(col("brand").isNull() | (col("brand") == ""), "unknown").otherwise(col("brand"))
        )
        .withColumn("price_clean", regexp_replace(col("price"), "[$,]", ""))
    )


def clean_trends(df: DataFrame) -> DataFrame:
    return (
        df
        .dropna()
        .withColumnRenamed("electronics 2018", "trend_score")
        .withColumn("trend_year", year(col("Time")))
        .withColumn("trend_month", month(col("Time")))
    )


def clean_commoncrawl(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(col("value").isNotNull())
        .withColumn("text_clean", lower(regexp_replace(col("value"), "[^a-zA-Z0-9\\s]", " ")))
        .filter(length(col("text_clean")) > 20)
    )
