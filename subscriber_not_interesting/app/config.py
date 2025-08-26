
"""
config.py (not_interesting)
---------------------------
Configuration for the 'not_interesting' subscriber service.
"""

import os
from typing import List


class SubscriberConfig:
	def __init__(self) -> None:
		self.KAFKA_BOOTSTRAP: List[str] = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
		self.KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")
		self.GROUP_ID: str = os.getenv("GROUP_ID", "not-interesting-subscriber-group")
		self.MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
		self.MONGO_DB: str = os.getenv("MONGO_DB", "newsdb")
		self.COLLECTION_NAME: str = os.getenv("MONGO_COLL", "not_interesting_messages")
		self.AUTO_OFFSET_RESET: str = os.getenv("AUTO_OFFSET_RESET", "earliest")
		self.MAX_POLL_INTERVAL_MS: int = int(os.getenv("MAX_POLL_INTERVAL_MS", "300000"))
		self.SESSION_TIMEOUT_MS: int = int(os.getenv("SESSION_TIMEOUT_MS", "10000"))


_CFG = SubscriberConfig()
KAFKA_BOOTSTRAP = _CFG.KAFKA_BOOTSTRAP
KAFKA_TOPIC = _CFG.KAFKA_TOPIC
GROUP_ID = _CFG.GROUP_ID
MONGO_URI = _CFG.MONGO_URI
MONGO_DB = _CFG.MONGO_DB
COLLECTION_NAME = _CFG.COLLECTION_NAME
AUTO_OFFSET_RESET = _CFG.AUTO_OFFSET_RESET
MAX_POLL_INTERVAL_MS = _CFG.MAX_POLL_INTERVAL_MS
SESSION_TIMEOUT_MS = _CFG.SESSION_TIMEOUT_MS
