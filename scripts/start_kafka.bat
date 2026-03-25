@echo off
echo ========================================
echo  Starting Kafka (Zookeeper + Broker)
echo ========================================
echo.
echo NOTE: Set KAFKA_HOME to your Kafka installation directory
echo Example: set KAFKA_HOME=C:\kafka
echo.

if "%KAFKA_HOME%"=="" (
    echo ERROR: KAFKA_HOME is not set!
    echo Please set it: set KAFKA_HOME=C:\path\to\kafka
    echo.
    echo Download Kafka: https://kafka.apache.org/downloads
    exit /b 1
)

echo Starting Zookeeper...
start "Zookeeper" %KAFKA_HOME%\bin\windows\zookeeper-server-start.bat %KAFKA_HOME%\config\zookeeper.properties

echo Waiting for Zookeeper (5s)...
timeout /t 5 /nobreak > nul

echo Starting Kafka Broker...
start "Kafka" %KAFKA_HOME%\bin\windows\kafka-server-start.bat %KAFKA_HOME%\config\server.properties

echo.
echo ========================================
echo  Kafka should be starting...
echo  Zookeeper: localhost:2181
echo  Kafka:     localhost:9092
echo ========================================
echo.
echo Next steps:
echo   1. python ingestion/kafka_setup.py  (create topic)
echo   2. python scripts/start_all.py      (start pipeline)
echo.
