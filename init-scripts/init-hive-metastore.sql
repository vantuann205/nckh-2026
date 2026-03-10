-- Initialize Hive Metastore Database
CREATE DATABASE IF NOT EXISTS hive_metastore;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO traffic;

-- Create traffic analytics database
CREATE DATABASE IF NOT EXISTS traffic_analytics;
GRANT ALL PRIVILEGES ON DATABASE traffic_analytics TO traffic;