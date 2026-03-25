"""
Kafka Setup — Create topic & verify connectivity
Topic: traffic-stream, ≥6 partitions, retention 1h
"""

import sys
import time
import logging
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("KafkaSetup")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_NAME = "traffic-stream"
NUM_PARTITIONS = 6
REPLICATION_FACTOR = 1  # Local dev = 1
RETENTION_MS = 3600000  # 1 hour


def check_kafka_connection(retries=5, delay=3):
    """Check if Kafka broker is reachable"""
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=5000,
            )
            consumer.topics()
            consumer.close()
            logger.info(f"✅ Kafka broker reachable at {KAFKA_BOOTSTRAP_SERVERS}")
            return True
        except NoBrokersAvailable:
            logger.warning(f"⏳ Attempt {attempt}/{retries} — Kafka not available. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            time.sleep(delay)

    logger.error(f"❌ Cannot connect to Kafka at {KAFKA_BOOTSTRAP_SERVERS} after {retries} attempts")
    return False


def create_topic():
    """Create Kafka topic with ≥6 partitions"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=10000,
        )

        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR,
            topic_configs={
                "retention.ms": str(RETENTION_MS),
                "cleanup.policy": "delete",
                "segment.ms": str(600000),  # 10 min segments
            },
        )

        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"✅ Topic '{TOPIC_NAME}' created successfully")
        logger.info(f"   Partitions: {NUM_PARTITIONS}")
        logger.info(f"   Retention: {RETENTION_MS // 1000}s ({RETENTION_MS // 3600000}h)")

        admin_client.close()
        return True

    except TopicAlreadyExistsError:
        logger.info(f"ℹ️  Topic '{TOPIC_NAME}' already exists")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to create topic: {e}")
        return False


def list_topics():
    """List all Kafka topics"""
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,
        )
        topics = consumer.topics()
        logger.info(f"📋 Existing topics: {topics}")
        consumer.close()
        return topics
    except Exception as e:
        logger.error(f"❌ Error listing topics: {e}")
        return set()


def setup():
    """Full Kafka setup"""
    logger.info("=" * 50)
    logger.info("🔧 KAFKA SETUP")
    logger.info("=" * 50)

    # Step 1: Check connection
    if not check_kafka_connection():
        logger.error("💡 Hãy đảm bảo Kafka đang chạy:")
        logger.error("   1. Start Zookeeper: bin\\windows\\zookeeper-server-start.bat config\\zookeeper.properties")
        logger.error("   2. Start Kafka:     bin\\windows\\kafka-server-start.bat config\\server.properties")
        sys.exit(1)

    # Step 2: Create topic
    create_topic()

    # Step 3: List topics
    list_topics()

    logger.info("=" * 50)
    logger.info("✅ KAFKA SETUP COMPLETE")
    logger.info("=" * 50)


if __name__ == "__main__":
    setup()
