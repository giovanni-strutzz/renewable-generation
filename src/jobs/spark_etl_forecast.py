import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, lit, round, when

MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin@mongodb:27017/energy_db?authSource=admin")
BUCKET_S3 = os.getenv("BUCKET_S3", "s3a://raw-zone/weather-data/*.json")

JAR_DIR = os.path.expanduser("~/spark-jars")
JARS = ",".join([
    f"{JAR_DIR}/hadoop-aws-3.3.4.jar",
    f"{JAR_DIR}/bundle-2.20.18.jar",
    f"{JAR_DIR}/mongo-spark-connector_2.12-10.4.0.jar",
])

FACTOR = 2.5

def create_spark_session() -> SparkSession:
    """
    Create Spark session
    """
    return SparkSession.builder \
        .appName("Energy_Forecast_ETL") \
        .config("spark.mongodb.write.connection.uri", "mongodb://admin:admin@mongodb:27017/energy_db?authSource=admin") \
        .config("spark.mongodb.write.connection.uri", MONGO_URI) \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def process_weather_data(spark: SparkSession):
    """
    Start the ETL process
    """
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.connection.timeout", "60000")
    hadoop_conf.set("fs.s3a.socket.timeout", "60000")
    hadoop_conf.set("fs.s3a.connection.request.timeout", "60000")
    hadoop_conf.set("fs.s3a.attempts.maximum", "3")

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
        lit("wind").alias("source"),
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
    .option("collection", "forecasts") \
    .save()

    print("Finished ETL process.")

if __name__ == "__main__":
    spark_session = create_spark_session()
    process_weather_data(spark_session)
    spark_session.stop()