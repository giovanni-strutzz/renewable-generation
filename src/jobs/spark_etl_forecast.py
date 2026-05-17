import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, lit, round, when

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/energy-db.forecasts")
BUCKET_S3 = os.getenv("BUCKET_S3", "s3a://raw-zone/weather-data/*.json")

FACTOR = 2.5

def create_spark_session() -> SparkSession:
    """
    Create Spark session
    """
    return SparkSession.builder \
        .appName("Energy_Forecast_ETL") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .getOrCreate()

def process_weather_data(spark: SparkSession):
    """
    Start the ETL process
    """
    print("Starting ETL process...")

    raw_df = spark.read.option("multiline", "true").json(BUCKET_S3)

    flattened_df = raw_df.withColumn(
        "zipped_data",
        arrays_zip(
            col("raw_payload.hourly.time"),
            col("raw_payload.hourly.wind_speed_100m")
        )
    ).withColumn("hourly_data", explode(col("zipped_data")))
    processed_df = flattened_df.select(
        col("park_id"),
        col("wind").alias("source"),
        col("hourly_data.time").cast("timestamp").alias("forecast_date"),
        col("hourly_data.wind_speed_100m").alias("wind_speed"),
    )

    final_df = processed_df.withColumn(
        "estimated_generation_mwh",
        when(
            (col("wind_speed") > 3) & (col("wind_speed") < 25),
            round(col("wind_speed") * FACTOR, 2)
        ).otherwise(0.0)
    ).drop("wind_speed")

    print("Saving transformed data on MongoDB...")

    final_df.write \
    .format("mongodb") \
    .mode("append") \
    .option("database", "energy_db") \
    .save()

    print("Finished ETL process.")

if __name__ == "__main__":
    spark_session = create_spark_session()
    process_weather_data(spark_session)
    spark_session.stop()