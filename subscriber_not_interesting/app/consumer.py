
"""
consumer.py (not_interesting)
-----------------------------
Kafka consumer logic for the 'not_interesting' subscriber.
Spawns a background loop to read messages and store them in MongoDB.
"""

import json
import time
from datetime import datetime, timezone
from typing import Dict, Any

from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, errors

from . import config


def _iso_utc_from_kafka_ts(ts_ms: int | None) -> str:
    if ts_ms is None:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


class NotInterestingConsumerService:
    """
    OOP consumer service for the 'not_interesting' topic with minimal API.
    """

    def __init__(self) -> None:
        self._consumer = KafkaConsumer(
            config.KAFKA_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            enable_auto_commit=False,
            auto_offset_reset=config.AUTO_OFFSET_RESET,
            group_id=config.GROUP_ID,
            max_poll_interval_ms=config.MAX_POLL_INTERVAL_MS,
            session_timeout_ms=config.SESSION_TIMEOUT_MS,
        )
        self._client = MongoClient(config.MONGO_URI, tz_aware=True, uuidRepresentation="standard")
        self._coll = self._client[config.MONGO_DB][config.COLLECTION_NAME]
        self._coll.create_index([("partition", ASCENDING), ("kafka_offset", ASCENDING)], unique=True)
        self._coll.create_index([("timestamp", ASCENDING)])

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    @property
    def collection(self):
        return self._coll

    def consume_once(self) -> None:
        for msg in self._consumer:
            try:
                doc: Dict[str, Any] = {
                    "partition": msg.partition,
                    "kafka_offset": msg.offset,
                    "topic": msg.topic,
                    "timestamp": _iso_utc_from_kafka_ts(msg.timestamp),
                    "value": msg.value,
                }
                self._coll.insert_one(doc)
                self._consumer.commit()
            except errors.DuplicateKeyError:
                self._consumer.commit()
            except Exception:
                # Do not commit — message will be retried
                pass
            time.sleep(0.01)


def get_collection():
    return NotInterestingConsumerService().collection


def build_consumer() -> KafkaConsumer:
    return NotInterestingConsumerService().consumer


def consume_once(consumer: KafkaConsumer, coll) -> None:
    service = NotInterestingConsumerService()
    service.consume_once()
