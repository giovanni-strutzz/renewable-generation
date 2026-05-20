import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, arrays_zip, lit, round, when

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@mongodb:27017/energy_db?authSource=admin")
BUCKET_S3 = os.getenv("BUCKET_S3", "s3a://raw-zone/weather-data/*.json")
AWS_ENDPOINT = f"http://localstack:{os.getenv('LOCALSTACK_PORT', '4566')}"

FACTOR = 2.5

def transform_weather_data(df: DataFrame) -> DataFrame:
    """
    Pure transformation logic: Flattening and Generation Calculation
    """
    flattened = df.withColumn(
        "zipped_data",
        arrays_zip(
            col("raw_payload.hourly.time"),
            col("raw_payload.hourly.wind_speed_100m")
        )
    ).withColumn("hourly_data", explode(col("zipped_data")))

    processed = flattened.select(
        col("park_id"),
        lit("wind").alias("source"),
        col("hourly_data.time").cast("timestamp").alias("forecast_date"),
        col("hourly_data.wind_speed_100m").alias("wind_speed")
    )

    return processed.withColumn(
        "estimated_generation_mwh",
        when(
            (col("wind_speed") > 3) & (col("wind_speed") < 25),
            round(col("wind_speed") * FACTOR, 2)
        ).otherwise(0.0)
    ).drop("wind_speed")

def create_spark_session() -> SparkSession:
    return SparkSession.builder \
        .appName("Energy_Forecast_ETL") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.hadoop.fs.s3a.endpoint", AWS_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test")) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    try:
        print(f"Reading from {BUCKET_S3}...")
        raw_df = spark.read.option("multiline", "true").json(BUCKET_S3)

        # Calling the pure transformation
        final_df = transform_weather_data(raw_df)

        print("Saving to MongoDB...")
        final_df.write.format("mongodb").mode("append") \
            .option("database", "energy_db") \
            .option("collection", "forecasts").save()

        print("ETL Processed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()