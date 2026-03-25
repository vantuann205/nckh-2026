"""
Stream Processing Configuration
All tunable constants for the pipeline.
"""

# === Kafka ===
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "traffic-stream"
KAFKA_CONSUMER_GROUP = "traffic-processor-group"

# === Redis ===
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0

# Redis key TTLs (seconds)
ROAD_DATA_TTL = 120       # latest road data: 2 min
ROAD_WINDOW_TTL = 300     # rolling window: 5 min
SUMMARY_TTL = 30          # global summary: 30s
CONGESTED_TTL = 60        # congested set: 1 min

# Redis Pub/Sub channel
REDIS_CHANNEL = "traffic-updates"

# === Congestion Detection ===
CONGESTION_SPEED_THRESHOLD = 20       # km/h — below this = congested
CONGESTION_VEHICLE_THRESHOLD = 50     # vehicles — above this + low speed = congested
SLOW_SPEED_THRESHOLD = 40             # km/h — below this = slow traffic

# === Window Processing ===
WINDOW_SIZES = [5, 10, 60]           # seconds: 5s, 10s, 1min
PRIMARY_WINDOW = 5                    # primary aggregation window (seconds)

# === Consumer ===
CONSUMER_POLL_TIMEOUT_MS = 1000       # Kafka poll timeout
CONSUMER_MAX_POLL_RECORDS = 500       # max records per poll
AGGREGATION_INTERVAL = 2             # seconds between aggregation flushes
