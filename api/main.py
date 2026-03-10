"""
Analytics API Layer
FastAPI service for data access and analytics
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import pandas as pd
from pyspark.sql import SparkSession
from delta import *
import redis
import json
from datetime import datetime, timedelta

app = FastAPI(
    title="Smart Traffic Analytics API",
    description="Lakehouse Analytics API for Traffic Data",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variables
spark = None
redis_client = None

class TrafficAnalytics:
    def __init__(self):
        global spark, redis_client
        
        # Initialize Spark
        builder = SparkSession.builder \
            .appName("TrafficAnalyticsAPI") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
            .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        
        # Initialize Redis
        redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)
        
        # Delta Lake paths
        self.gold_path = "s3a://gold-layer/"
        self.silver_path = "s3a://silver-layer/"

analytics = TrafficAnalytics()

# Pydantic models
class KPIResponse(BaseModel):
    total_vehicles: int
    unique_vehicles: int
    avg_speed: float
    max_speed: float
    min_speed: float
    avg_fuel_level: float
    speeding_violations: int
    fuel_alerts: int
    high_congestion_count: int
    calculation_timestamp: str

class DistrictAnalytics(BaseModel):
    district: str
    vehicle_count: int
    unique_vehicles: int
    avg_speed: float
    avg_fuel_level: float
    congestion_incidents: int
    total_alerts: int

class VehicleTypeAnalytics(BaseModel):
    vehicle_type: str
    count: int
    avg_speed: float
    avg_passengers: float
    avg_fuel_level: float
    total_alerts: int

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    print("🚀 Analytics API starting up...")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if spark:
        spark.stop()
    print("🛑 Analytics API shut down")

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "message": "Smart Traffic Analytics API",
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Detailed health check"""
    try:
        # Test Spark connection
        spark_status = "healthy" if spark else "unhealthy"
        
        # Test Redis connection
        redis_status = "healthy"
        try:
            redis_client.ping()
        except:
            redis_status = "unhealthy"
        
        return {
            "status": "healthy",
            "services": {
                "spark": spark_status,
                "redis": redis_status
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/kpi", response_model=KPIResponse)
async def get_kpi_metrics():
    """Get overall KPI metrics"""
    try:
        # Check cache first
        cache_key = "kpi_metrics"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        # Query from Delta Lake
        kpi_df = spark.read.format("delta").load(f"{analytics.gold_path}kpi_metrics")
        kpi_data = kpi_df.collect()[0].asDict()
        
        # Convert to response model
        response = KPIResponse(**kpi_data)
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, response.json())
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching KPI metrics: {str(e)}")

@app.get("/api/v1/districts", response_model=List[DistrictAnalytics])
async def get_district_analytics(limit: int = Query(10, ge=1, le=50)):
    """Get district analytics"""
    try:
        cache_key = f"district_analytics_{limit}"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        district_df = spark.read.format("delta").load(f"{analytics.gold_path}district_analytics")
        district_data = district_df.orderBy("vehicle_count", ascending=False).limit(limit).collect()
        
        response = [DistrictAnalytics(**row.asDict()) for row in district_data]
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, json.dumps([r.dict() for r in response]))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching district analytics: {str(e)}")

@app.get("/api/v1/vehicle-types", response_model=List[VehicleTypeAnalytics])
async def get_vehicle_type_analytics():
    """Get vehicle type analytics"""
    try:
        cache_key = "vehicle_type_analytics"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        vehicle_df = spark.read.format("delta").load(f"{analytics.gold_path}vehicle_type_analytics")
        vehicle_data = vehicle_df.collect()
        
        response = [VehicleTypeAnalytics(**row.asDict()) for row in vehicle_data]
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, json.dumps([r.dict() for r in response]))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching vehicle type analytics: {str(e)}")

@app.get("/api/v1/hourly-patterns")
async def get_hourly_patterns():
    """Get hourly traffic patterns"""
    try:
        cache_key = "hourly_patterns"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        hourly_df = spark.read.format("delta").load(f"{analytics.gold_path}hourly_patterns")
        hourly_data = hourly_df.orderBy("hour").collect()
        
        response = [row.asDict() for row in hourly_data]
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, json.dumps(response))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching hourly patterns: {str(e)}")

@app.get("/api/v1/speed-distribution")
async def get_speed_distribution():
    """Get speed distribution"""
    try:
        cache_key = "speed_distribution"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        speed_df = spark.read.format("delta").load(f"{analytics.gold_path}speed_distribution")
        speed_data = speed_df.collect()
        
        response = [row.asDict() for row in speed_data]
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, json.dumps(response))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching speed distribution: {str(e)}")

@app.get("/api/v1/weather-impact")
async def get_weather_impact():
    """Get weather impact on traffic"""
    try:
        cache_key = "weather_impact"
        cached_data = redis_client.get(cache_key)
        
        if cached_data:
            return json.loads(cached_data)
        
        weather_df = spark.read.format("delta").load(f"{analytics.gold_path}weather_impact")
        weather_data = weather_df.collect()
        
        response = [row.asDict() for row in weather_data]
        
        # Cache for 5 minutes
        redis_client.setex(cache_key, 300, json.dumps(response))
        
        return response
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching weather impact: {str(e)}")

@app.get("/api/v1/raw-data")
async def get_raw_data(
    limit: int = Query(1000, ge=1, le=10000),
    vehicle_type: Optional[str] = None,
    district: Optional[str] = None,
    min_speed: Optional[float] = None,
    max_speed: Optional[float] = None
):
    """Get raw traffic data with filters"""
    try:
        # Read from Silver layer (cleaned data)
        df = spark.read.format("delta").load(f"{analytics.silver_path}traffic_cleaned")
        
        # Apply filters
        if vehicle_type:
            df = df.filter(df.vehicle_type == vehicle_type)
        if district:
            df = df.filter(df.district == district)
        if min_speed is not None:
            df = df.filter(df.speed_kmph >= min_speed)
        if max_speed is not None:
            df = df.filter(df.speed_kmph <= max_speed)
        
        # Select relevant columns and limit
        result_df = df.select(
            "vehicle_id", "vehicle_type", "speed_kmph", "fuel_level_percentage",
            "passenger_count", "district", "street", "congestion_level",
            "weather", "temperature", "humidity", "timestamp"
        ).limit(limit)
        
        # Convert to JSON
        data = [row.asDict() for row in result_df.collect()]
        
        return {
            "data": data,
            "count": len(data),
            "filters": {
                "vehicle_type": vehicle_type,
                "district": district,
                "min_speed": min_speed,
                "max_speed": max_speed
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching raw data: {str(e)}")

@app.post("/api/v1/refresh-cache")
async def refresh_cache():
    """Refresh all cached data"""
    try:
        # Clear all cache keys
        cache_keys = [
            "kpi_metrics",
            "district_analytics_*",
            "vehicle_type_analytics",
            "hourly_patterns",
            "speed_distribution",
            "weather_impact"
        ]
        
        for pattern in cache_keys:
            if "*" in pattern:
                keys = redis_client.keys(pattern)
                if keys:
                    redis_client.delete(*keys)
            else:
                redis_client.delete(pattern)
        
        return {
            "message": "Cache refreshed successfully",
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing cache: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)