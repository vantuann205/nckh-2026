#!/bin/bash

echo "🚀 Starting Smart Traffic Lakehouse System..."

# Build and start all services
echo "📦 Building Docker images..."
docker-compose build

echo "🔄 Starting infrastructure services..."
docker-compose up -d zookeeper kafka minio minio-setup postgres redis

echo "⏳ Waiting for infrastructure to be ready..."
sleep 30

echo "🔄 Starting processing services..."
docker-compose up -d spark-master spark-worker-1 spark-worker-2 hive-metastore

echo "⏳ Waiting for Spark cluster to be ready..."
sleep 20

echo "🔄 Starting ETL pipeline..."
docker-compose up -d etl-processor

echo "🔄 Starting Analytics API..."
docker-compose up -d analytics-api

echo "⏳ Waiting for API to be ready..."
sleep 10

echo "🔄 Starting Dashboard..."
docker-compose up -d dashboard

echo ""
echo "✅ Lakehouse System Started Successfully!"
echo ""
echo "🌐 Access Points:"
echo "   📊 Dashboard:        http://localhost:8501"
echo "   🔗 Analytics API:    http://localhost:8000"
echo "   ⚡ Spark Master UI:  http://localhost:8080"
echo "   💾 MinIO Console:    http://localhost:9001"
echo "   📡 Kafka Manager:    http://localhost:9092"
echo ""
echo "🔧 Admin Credentials:"
echo "   MinIO: minioadmin / minioadmin123"
echo "   PostgreSQL: traffic / traffic123"
echo ""
echo "📋 To view logs: docker-compose logs -f [service-name]"
echo "🛑 To stop: docker-compose down"
echo ""