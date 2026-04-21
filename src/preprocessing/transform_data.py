
from pyspark.sql.functions import col

def clean_reviews(df):
    return df.select(
        col("asin"),
        col("reviewerID"),
        col("overall"),
        col("reviewText"),
        col("unixReviewTime")
    )

