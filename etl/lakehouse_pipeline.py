"""
Lakehouse ETL Pipeline
Implements Bronze -> Silver -> Gold data processing layers
"""

import os
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import *
import boto3
from kafka import KafkaConsumer, KafkaProducer

class LakehousePipeline:
    def __init__(self):
        # Initialize Spark with Delta Lake
        builder = SparkSession.builder \
            .appName("TrafficLakehousePipeline") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.driver.memory", "2g") \
            .config("spark.executor.memory", "2g")
        
        self.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")
        
        # S3/MinIO paths
        self.bronze_path = "s3a://bronze-layer/"
        self.silver_path = "s3a://silver-layer/"
        self.gold_path = "s3a://gold-layer/"
        self.delta_path = "s3a://delta-lake/"
        
        print("✅ Lakehouse Pipeline initialized")
    
    def ingest_from_kafka(self):
        """Ingest real-time data from Kafka"""
        try:
            consumer = KafkaConsumer(
                'traffic-data',
                bootstrap_servers=['kafka:29092'],
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            
            print("🔄 Starting Kafka ingestion...")
            
            batch_data = []
            batch_size = 100
            
            for message in consumer:
                batch_data.append(message.value)
                
                if len(batch_data) >= batch_size:
                    self.process_batch(batch_data)
                    batch_data = []
                    
        except Exception as e:
            print(f"❌ Kafka ingestion error: {e}")
    
    def ingest_batch_data(self, file_path):
        """Ingest batch data from file"""
        try:
            print(f"📥 Ingesting batch data from {file_path}")
            
            # Read JSON data
            df = self.spark.read.json(file_path)
            
            # Add ingestion metadata
            df_with_metadata = df \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("ingestion_date", current_date()) \
                .withColumn("data_source", lit("batch_upload"))
            
            # Write to Bronze layer
            bronze_table_path = f"{self.bronze_path}traffic_raw"
            
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bronze_table_path)
            
            print(f"✅ Bronze layer: {df_with_metadata.count()} records ingested")
            
            # Trigger Silver processing
            self.process_silver_layer()
            
        except Exception as e:
            print(f"❌ Batch ingestion error: {e}")
    
    def process_silver_layer(self):
        """Process Bronze -> Silver (Data Cleaning & Transformation)"""
        try:
            print("🔄 Processing Silver layer...")
            
            # Read from Bronze
            bronze_df = self.spark.read.format("delta").load(f"{self.bronze_path}traffic_raw")
            
            # Data cleaning and transformation
            silver_df = bronze_df \
                .withColumn("timestamp", to_timestamp(col("timestamp"))) \
                .withColumn("date", to_date(col("timestamp"))) \
                .withColumn("hour", hour(col("timestamp"))) \
                .withColumn("day_of_week", dayofweek(col("timestamp"))) \
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
                           when(col("fuel_level_percentage") < 20, True).otherwise(False)) \
                .withColumn("speed_category",
                           when(col("speed_kmph") < 20, "Very Slow")
                           .when(col("speed_kmph") < 40, "Slow")
                           .when(col("speed_kmph") < 60, "Normal")
                           .when(col("speed_kmph") < 80, "Fast")
                           .otherwise("Very Fast")) \
                .withColumn("fuel_status",
                           when(col("fuel_level_percentage") < 10, "Critical")
                           .when(col("fuel_level_percentage") < 20, "Low")
                           .when(col("fuel_level_percentage") < 50, "Medium")
                           .otherwise("High")) \
                .withColumn("processed_timestamp", current_timestamp())
            
            # Write to Silver layer
            silver_table_path = f"{self.silver_path}traffic_cleaned"
            
            silver_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(silver_table_path)
            
            print(f"✅ Silver layer: {silver_df.count()} records processed")
            
            # Trigger Gold processing
            self.process_gold_layer()
            
        except Exception as e:
            print(f"❌ Silver processing error: {e}")
    
    def process_gold_layer(self):
        """Process Silver -> Gold (Business Aggregations)"""
        try:
            print("🔄 Processing Gold layer...")
            
            # Read from Silver
            silver_df = self.spark.read.format("delta").load(f"{self.silver_path}traffic_cleaned")
            
            # KPI Aggregations
            kpi_df = silver_df.agg(
                count("vehicle_id").alias("total_vehicles"),
                countDistinct("vehicle_id").alias("unique_vehicles"),
                avg("speed_kmph").alias("avg_speed"),
                max("speed_kmph").alias("max_speed"),
                min("speed_kmph").alias("min_speed"),
                avg("fuel_level_percentage").alias("avg_fuel_level"),
                sum(when(col("has_speeding_alert"), 1).otherwise(0)).alias("speeding_violations"),
                sum(when(col("has_fuel_alert"), 1).otherwise(0)).alias("fuel_alerts"),
                sum(when(col("congestion_level") == "High", 1).otherwise(0)).alias("high_congestion_count")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # District Analytics
            district_df = silver_df.groupBy("district").agg(
                count("vehicle_id").alias("vehicle_count"),
                countDistinct("vehicle_id").alias("unique_vehicles"),
                avg("speed_kmph").alias("avg_speed"),
                avg("fuel_level_percentage").alias("avg_fuel_level"),
                sum(when(col("congestion_level") == "High", 1).otherwise(0)).alias("congestion_incidents"),
                sum("alert_count").alias("total_alerts")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # Vehicle Type Analytics
            vehicle_type_df = silver_df.groupBy("vehicle_type").agg(
                count("vehicle_id").alias("count"),
                avg("speed_kmph").alias("avg_speed"),
                avg("passenger_count").alias("avg_passengers"),
                avg("fuel_level_percentage").alias("avg_fuel_level"),
                sum("alert_count").alias("total_alerts")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # Hourly Traffic Pattern
            hourly_df = silver_df.groupBy("hour").agg(
                count("vehicle_id").alias("vehicle_count"),
                avg("speed_kmph").alias("avg_speed"),
                sum(when(col("congestion_level") == "High", 1).otherwise(0)).alias("congestion_count")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # Speed Distribution
            speed_distribution_df = silver_df.groupBy("speed_category").agg(
                count("vehicle_id").alias("count"),
                avg("speed_kmph").alias("avg_speed")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # Weather Impact
            weather_impact_df = silver_df.groupBy("weather").agg(
                count("vehicle_id").alias("vehicle_count"),
                avg("speed_kmph").alias("avg_speed"),
                sum(when(col("congestion_level") == "High", 1).otherwise(0)).alias("congestion_incidents")
            ).withColumn("calculation_timestamp", current_timestamp())
            
            # Write Gold tables
            gold_tables = {
                "kpi_metrics": kpi_df,
                "district_analytics": district_df,
                "vehicle_type_analytics": vehicle_type_df,
                "hourly_patterns": hourly_df,
                "speed_distribution": speed_distribution_df,
                "weather_impact": weather_impact_df
            }
            
            for table_name, df in gold_tables.items():
                table_path = f"{self.gold_path}{table_name}"
                df.write \
                    .format("delta") \
                    .mode("overwrite") \
                    .save(table_path)
                
                print(f"✅ Gold table '{table_name}': {df.count()} records")
            
            print("✅ Gold layer processing completed")
            
        except Exception as e:
            print(f"❌ Gold processing error: {e}")
    
    def create_delta_tables(self):
        """Create Delta Lake tables for better performance"""
        try:
            print("🔄 Creating Delta Lake tables...")
            
            # Create database
            self.spark.sql("CREATE DATABASE IF NOT EXISTS traffic_lakehouse")
            
            # Register Delta tables
            tables = [
                ("traffic_lakehouse.bronze_raw", f"{self.bronze_path}traffic_raw"),
                ("traffic_lakehouse.silver_cleaned", f"{self.silver_path}traffic_cleaned"),
                ("traffic_lakehouse.gold_kpi", f"{self.gold_path}kpi_metrics"),
                ("traffic_lakehouse.gold_district", f"{self.gold_path}district_analytics"),
                ("traffic_lakehouse.gold_vehicle_type", f"{self.gold_path}vehicle_type_analytics"),
                ("traffic_lakehouse.gold_hourly", f"{self.gold_path}hourly_patterns"),
                ("traffic_lakehouse.gold_speed", f"{self.gold_path}speed_distribution"),
                ("traffic_lakehouse.gold_weather", f"{self.gold_path}weather_impact")
            ]
            
            for table_name, table_path in tables:
                try:
                    self.spark.sql(f"""
                        CREATE TABLE IF NOT EXISTS {table_name}
                        USING DELTA
                        LOCATION '{table_path}'
                    """)
                    print(f"✅ Created table: {table_name}")
                except Exception as e:
                    print(f"⚠️ Table {table_name} might already exist: {e}")
            
        except Exception as e:
            print(f"❌ Delta table creation error: {e}")
    
    def run_pipeline(self):
        """Run the complete pipeline"""
        print("🚀 Starting Lakehouse Pipeline...")
        
        # Create Delta tables
        self.create_delta_tables()
        
        # Check for batch data
        batch_file = "/data/traffic_data_0.json"
        if os.path.exists(batch_file):
            self.ingest_batch_data(batch_file)
        
        # Start Kafka consumer (runs continuously)
        # self.ingest_from_kafka()
        
        print("✅ Pipeline execution completed")
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    pipeline = LakehousePipeline()
    
    try:
        pipeline.run_pipeline()
        
        # Keep running for real-time processing
        print("🔄 Pipeline running... Press Ctrl+C to stop")
        while True:
            time.sleep(60)  # Run every minute
            print(f"⏰ Pipeline heartbeat: {datetime.now()}")
            
    except KeyboardInterrupt:
        print("🛑 Pipeline stopped by user")
    except Exception as e:
        print(f"❌ Pipeline error: {e}")
    finally:
        pipeline.stop()