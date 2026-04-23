
from pyspark.sql import SparkSession


def save_parquet(df, output_path: str, label: str):
    print(f"\nSaving {label} to: {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved {label} successfully.")


def verify_parquet_readback(spark: SparkSession, output_path: str, label: str):
    print(f"\nVerifying read-back for {label} from: {output_path}")
    check_df = spark.read.parquet(output_path)
    check_df.printSchema()
    check_df.show(5, truncate=False)
    print(f"Verification sample count: {check_df.limit(1000).count()}")


if __name__ == "__main__":
    print("This module is intended to be imported by src/main.py")
