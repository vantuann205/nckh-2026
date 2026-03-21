from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import os

class TrafficDataProcessor
    def __init__(self):
        builder = SparkSession.builder \
            .appName("TrafficBigDataAnalytics") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
    
    def load_json_data(self, file_path):
        """Load JSON data into Spark DataFrame"""
        schema = StructType([
            StructField("vehicle_id", StringType(), True),
            StructField("owner", StructType([
                StructField("name", StringType(), True),
                StructField("license_number", StringType(), True),
                StructField("contact_info", StructType([
                    StructField("phone", StringType(), True),
                    StructField("email", StringType(), True)
                ]), True)
            ]), True),
            StructField("speed_kmph", DoubleType(), True),
            StructField("road", StructType([
                StructField("street", StringType(), True),
                StructField("district", StringType(), True),
                StructField("city", StringType(), True)
            ]), True),
            StructField("timestamp", StringType(), True),
            StructField("vehicle_type", StringType(), True),
            StructField("vehicle_classification", StringType(), True),
            StructField("coordinates", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("fuel_level_percentage", IntegerType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("weather_condition", StructType([
                StructField("temperature_celsius", DoubleType(), True),
                StructField("humidity_percentage", DoubleType(), True),
                StructField("condition", StringType(), True)
            ]), True),
            StructField("traffic_status", StructType([
                StructField("congestion_level", StringType(), True),
                StructField("estimated_delay_minutes", IntegerType(), True)
            ]), True),
            StructField("alerts", ArrayType(StructType([
                StructField("type", StringType(), True),
                StructField("description", StringType(), True),
                StructField("severity", StringType(), True),
                StructField("timestamp", StringType(), True)
            ])), True)
        ])
        
        df = self.spark.read.schema(schema).json(file_path)
        return df
    
    def process_bronze_layer(self, df):
        """Bronze Layer: Raw data ingestion"""
        bronze_df = df.withColumn("ingestion_timestamp", current_timestamp())
        return bronze_df
    
    def process_silver_layer(self, bronze_df):
        """Silver Layer: Cleaned and transformed data"""
        silver_df = bronze_df \
            .withColumn("timestamp", to_timestamp(col("timestamp"))) \
            .withColumn("date", to_date(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("owner_name", col("owner.name")) \
            .withColumn("license_number", col("owner.license_number")) \
            .withColumn("street", col("road.street")) \
            .withColumn("district", col("road.district")) \
            .withColumn("city", col("road.city")) \
            .withColumn("latitude", col("coordinates.latitude")) \
            .withColumn("longitude", col("coordinates.longitude")) \
            .withColumn("temperature", col("weather_condition.temperature_celsius")) \
            .withColumn("humidity", col("weather_condition.humidity_percentage")) \
            .withColumn("weather", col("weather_condition.condition")) \
            .withColumn("congestion_level", col("traffic_status.congestion_level")) \
            .withColumn("delay_minutes", col("traffic_status.estimated_delay_minutes")) \
            .withColumn("alert_count", size(col("alerts"))) \
            .withColumn("has_speeding_alert", 
                       when(array_contains(col("alerts.type"), "Speeding"), True).otherwise(False)) \
            .withColumn("has_fuel_alert",
                       when(col("fuel_level_percentage") < 20, True).otherwise(False))
        
        return silver_df
    
    def process_gold_layer(self, silver_df):
        """Gold Layer: Business-level aggregations"""
        # KPI Metrics
        kpi_df = silver_df.agg(
            count("vehicle_id").alias("total_vehicles"),
            avg("speed_kmph").alias("avg_speed"),
            countDistinct(when(col("timestamp") >= date_sub(current_timestamp(), 1), 
                              col("vehicle_id"))).alias("active_vehicles"),
            sum(when(col("has_speeding_alert"), 1).otherwise(0)).alias("speeding_alerts"),
            sum(when(col("has_fuel_alert"), 1).otherwise(0)).alias("fuel_alerts")
        )
        
        # District Analytics
        district_df = silver_df.groupBy("district").agg(
            count("vehicle_id").alias("vehicle_count"),
            avg("speed_kmph").alias("avg_speed"),
            sum(when(col("congestion_level") == "High", 1).otherwise(0)).alias("congestion_count")
        ).orderBy(desc("vehicle_count"))
        
        # Vehicle Type Analytics
        vehicle_type_df = silver_df.groupBy("vehicle_type").agg(
            count("vehicle_id").alias("count"),
            avg("speed_kmph").alias("avg_speed"),
            avg("passenger_count").alias("avg_passengers")
        )
        
        # Hourly Traffic Pattern
        hourly_df = silver_df.groupBy("hour").agg(
            count("vehicle_id").alias("vehicle_count"),
            avg("speed_kmph").alias("avg_speed")
        ).orderBy("hour")
        
        # Speed Distribution
        speed_bins = [0, 20, 40, 60, 80, 120]
        speed_df = silver_df.withColumn(
            "speed_range",
            when(col("speed_kmph") < 20, "0-20")
            .when((col("speed_kmph") >= 20) & (col("speed_kmph") < 40), "20-40")
            .when((col("speed_kmph") >= 40) & (col("speed_kmph") < 60), "40-60")
            .when((col("speed_kmph") >= 60) & (col("speed_kmph") < 80), "60-80")
            .otherwise("80+")
        ).groupBy("speed_range").count().orderBy("speed_range")
        
        return {
            "kpi": kpi_df,
            "district": district_df,
            "vehicle_type": vehicle_type_df,
            "hourly": hourly_df,
            "speed_distribution": speed_df
        }
    
    def get_map_data(self, silver_df, limit=1000):
        """Get data for map visualization"""
        map_df = silver_df.select(
            "vehicle_id", "vehicle_type", "speed_kmph",
            "latitude", "longitude", "street", "district",
            "congestion_level", "has_speeding_alert"
        ).limit(limit)
        
        return map_df.toPandas()
    
    def query_data(self, silver_df, filters=None):
        """Query data with filters"""
        df = silver_df
        
        if filters:
            if "vehicle_type" in filters:
                df = df.filter(col("vehicle_type") == filters["vehicle_type"])
            if "district" in filters:
                df = df.filter(col("district") == filters["district"])
            if "min_speed" in filters:
                df = df.filter(col("speed_kmph") >= filters["min_speed"])
            if "max_speed" in filters:
                df = df.filter(col("speed_kmph") <= filters["max_speed"])
        
        return df
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
