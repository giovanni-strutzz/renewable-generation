import json
import os
import socket

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, arrays_zip, lit, round, when
from datetime import datetime
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@mongodb:27017/energy_db?authSource=admin")
BUCKET_S3 = os.getenv("BUCKET_S3", "s3a://raw-zone/weather-data/*.json")
BUCKET_S3_SILVER = "s3a://lakeouse/silver/weather_data"

JAR_DIR = os.path.expanduser("~/spark-jars")
JARS = ",".join([
    f"{JAR_DIR}/hadoop-aws-3.3.4.jar",
    f"{JAR_DIR}/bundle-2.20.18.jar",
    f"{JAR_DIR}/mongo-spark-connector_2.12-10.4.0.jar",
])

FACTOR = 2.5

class LogstashConfig:
    def __init__(self, host='logstash', port=5000):
        self.host = host
        self.port = port

    def send_logs(self, level, message, extra=None):
        payload = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "app": "Energy_Forecast_ETL",
            "message": message,
            "extra": extra or {}
        }

        print(f"[{level}] {message}")

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(1)
                sock.connect((self.host, self.port))
                sock.sendall(json.dumps(payload).encode('utf-8'))
        except Exception as exc:
            raise Exception(str(exc))


logger = LogstashConfig()

def create_spark_session() -> SparkSession:
    """
    Create Spark session
    """
    return SparkSession.builder \
        .appName("Energy_Forecast_ETL") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


def validate_data_quality(df: DataFrame):
    logger.send_logs("INFO", "Starting Data Quality")

    ge_df = SparkDFDataset(df)

    ge_df.expect_column_values_to_not_be_null("park_id")
    ge_df.expect_column_values_to_be_between("estimated_generation_mwh", min_value=0, max_value=500)
    ge_df.expect_column_to_exist("forecast_date")

    validation_result = ge_df.validate()

    if not validation_result["success"]:
        logger.send_logs("CRITICAL", "Data Quality Audit Failed", {"results": str(validation_result)})
        raise ValueError("Pipeline Error: Failed on Data Contract. Please review DTOs")

    logger.send_logs("INFO", "Data Quality Audit Passed")


def transform_weather_data(df: DataFrame) -> DataFrame:
    flattened_df = df.withColumn(
        "zipped_data",
        arrays_zip(
            col("raw_payload.hourly.time"),
            col("raw_payload.hourly.wind_speed_100m")
        )
    ).withColumn("hourly_data", explode(col("zipped_data")))

    processed_df = flattened_df.select(
        col("park_id"),
        lit("wind").alias("source"),
        col("hourly_data.time").cast("timestamp").alias("forecast_date"),
        col("hourly_data.wind_speed_100m").alias("wind_speed"),
        lit(datetime.now().strftime("%Y-%m-%d")).alias("execution_date")
    )

    return processed_df.withColumn(
        "estimated_generation_mwh",
        when(
            (col("wind_speed") > 3) & (col("wind_speed") < 25),
            round(col("wind_speed") * FACTOR, 2)
        ).otherwise(0.0)
    )


def process_pipeline(spark: SparkSession):
    try:
        logger.send_logs("INFO", "Starting Process -> Delta Lake -> Mongo")

        raw_df = spark.read.option("multiline", "true").json(BUCKET_S3)

        transformed_df = transform_weather_data(raw_df)

        validate_data_quality(transformed_df)

        logger.send_logs("INFO", "Saving data on Silver Layer")
        transformed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("partitionOverwriteMode", "dynamic") \
            .partitionBy("execution_date") \
            .save(BUCKET_S3_SILVER)

        logger.send_logs("INFO", "Syncing data with MongoDB ")
        transformed_df.write \
            .format("mongodb") \
            .mode("append") \
            .option("database", "energy_db") \
            .option("collection", "forecasts") \
            .save()

        logger.send_logs("INFO", "Finished ETL process sucessfully")

    except Exception as e:
        logger.send_logs("ERROR", f"Falha no Pipeline: {str(e)}")
        raise


def main():
    spark_session = create_spark_session()
    try:
        process_pipeline(spark_session)
    finally:
        spark_session.stop()


if __name__ == "__main__":
    main()