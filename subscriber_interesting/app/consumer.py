import json
import time
from datetime import datetime, timezone
from typing import Dict, Any
from kafka import KafkaConsumer
from pymongo import MongoClient, ASCENDING, errors
from . import config


class InterestingConsumerService:
    """
    OOP consumer service for the 'interesting' topic.
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
        self._coll.create_index([("kafka_offset", ASCENDING)], unique=True)
        self._coll.create_index([("timestamp", ASCENDING)])

    @property
    def consumer(self) -> KafkaConsumer:
        return self._consumer

    @property
    def collection(self):
        return self._coll
    
    def _convert_kafka_ts_to_iso_utc(self,ts_ms: int | None) -> str:
        if ts_ms is None:
            return datetime.now(timezone.utc).isoformat()
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()

    def consume_once(self) -> None:
        """
        
        """
        for msg in self._consumer:
            try:
                doc: Dict[str, Any] = {
                    "kafka_offset": msg.offset,
                    "topic": msg.topic,
                    "timestamp": self._convert_kafka_ts_to_iso_utc(msg.timestamp),
                    "value": msg.value,
                }
                self._coll.insert_one(doc)
                self._consumer.commit()
            except errors.DuplicateKeyError:
                self._consumer.commit()
            except Exception:
                pass
            time.sleep(0.01)