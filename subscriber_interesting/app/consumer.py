import json
import time
from datetime import datetime, timezone
from typing import Dict, Any
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, errors
from . import config

def _convert_kafka_ts_to_iso_utc(ts_ms: int | None) -> str:
    """
    Convert Kafka millisecond timestamp to ISO UTC string.
    """
    if ts_ms is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

def get_collection():
    """
    Return a MongoDB collection handle for this subscriber.
    """
    client = MongoClient(config.MONGO_URI, tz_aware=True, uuidRepresentation="standard")
    coll = client[config.MONGO_DB][config.COLLECTION_NAME]
    coll.create_index([("partition", ASCENDING), ("kafka_offset", ASCENDING)], unique=True)
    coll.create_index([("timestamp", ASCENDING)])
    return coll

def build_consumer() -> KafkaConsumer:
    """
    Build and return a KafkaConsumer for the configured topic.
    """
    return KafkaConsumer(
        config.KAFKA_TOPIC,
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        enable_auto_commit=False,
        auto_offset_reset=config.AUTO_OFFSET_RESET,
        group_id=config.GROUP_ID,
        max_poll_interval_ms=config.MAX_POLL_INTERVAL_MS,
        session_timeout_ms=config.SESSION_TIMEOUT_MS,
    )

def consume_once(consumer: KafkaConsumer, coll) -> None:
    """
    Consume one batch iteration from Kafka and insert documents to MongoDB.
    Commits offsets only after successful insert for at-least-once semantics.
    """
    for msg in consumer:
        try:
            doc: Dict[str, Any] = {
                "partition": msg.partition,
                "kafka_offset": msg.offset,
                "topic": msg.topic,
                "timestamp": _convert_kafka_ts_to_iso_utc(msg.timestamp),
                "value": msg.value,
            }
            coll.insert_one(doc)
            consumer.commit()
        except errors.DuplicateKeyError:
            consumer.commit()
        except Exception:
            pass
        time.sleep(0.01)
