from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import os

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "traffic-data"
CHECKPOINT_DIR = "f:/nckh-2026/analyse-data/lakehouse/checkpoints"
DELTA_PATH = "f:/nckh-2026/analyse-data/lakehouse/delta"

def get_spark_session():
    builder = SparkSession.builder \
        .appName("TrafficStreamingProcessor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_streaming_pipeline():
    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"🚀 Starting Spark Structured Streaming from Kafka topic: {KAFKA_TOPIC}")

    # 1. Read from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON
    schema = StructType([
        StructField("vehicle_id", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("speed_kmph", DoubleType(), True),
        StructField("road", StructType([
            StructField("street", StringType(), True),
            StructField("district", StringType(), True)
        ]), True),
        StructField("timestamp", StringType(), True),
        StructField("fuel_level_percentage", IntegerType(), True),
        StructField("traffic_status", StructType([
            StructField("congestion_level", StringType(), True)
        ]), True)
    ])

    parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # 3. Bronze Table: Write Raw Data
    bronze_query = parsed_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/bronze") \
        .start(f"{DELTA_PATH}/bronze_traffic")

    # 4. Silver Table: Clean & Transform
    silver_df = parsed_df \
        .withColumn("ts", to_timestamp(col("timestamp"))) \
        .withColumn("street", col("road.street")) \
        .withColumn("district", col("road.district")) \
        .drop("road", "timestamp") \
        .withColumn("ingestion_time", current_timestamp())

    silver_query = silver_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/silver") \
        .start(f"{DELTA_PATH}/silver_traffic")

    # 5. Gold Table: Real-time Aggregations (KPIs)
    # Note: For real-time updates < 5s, we can use a small trigger interval
    gold_df = silver_df \
        .withWatermark("ts", "10 minutes") \
        .groupBy(window("ts", "1 minute")).agg(
            count("vehicle_id").alias("total_vehicles"),
            avg("speed_kmph").alias("avg_speed"),
            count(when(col("speed_kmph") > 80, 1)).alias("alerts")
        )

    gold_query = gold_df.writeStream \
        .format("delta") \
        .outputMode("complete") \
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/gold") \
        .trigger(processingTime='2 seconds') \
        .start(f"{DELTA_PATH}/gold_kpis")

    print("✅ Streaming queries started.")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    run_streaming_pipeline()
