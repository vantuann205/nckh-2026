@echo off
echo 🚀 Starting Smart Traffic Lakehouse System...

echo 📦 Building Docker images...
docker-compose build

echo 🔄 Starting infrastructure services...
docker-compose up -d zookeeper kafka minio minio-setup postgres redis

echo ⏳ Waiting for infrastructure to be ready...
timeout /t 30 /nobreak > nul

echo 🔄 Starting processing services...
docker-compose up -d spark-master spark-worker-1 spark-worker-2

echo ⏳ Waiting for Spark cluster to be ready...
timeout /t 20 /nobreak > nul

echo 🔄 Starting ETL pipeline...
docker-compose up -d etl-processor

echo 🔄 Starting Analytics API...
docker-compose up -d analytics-api

echo ⏳ Waiting for API to be ready...
timeout /t 10 /nobreak > nul

echo 🔄 Starting Dashboard...
docker-compose up -d dashboard

echo.
echo ✅ Lakehouse System Started Successfully!
echo.
echo 🌐 Access Points:
echo    📊 Dashboard:        http://localhost:8501
echo    🔗 Analytics API:    http://localhost:8000
echo    ⚡ Spark Master UI:  http://localhost:8080
echo    💾 MinIO Console:    http://localhost:9001
echo.
echo 🔧 Admin Credentials:
echo    MinIO: minioadmin / minioadmin123
echo    PostgreSQL: traffic / traffic123
echo.
echo 📋 To view logs: docker-compose logs -f [service-name]
echo 🛑 To stop: docker-compose down
echo.