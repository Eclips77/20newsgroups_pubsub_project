
"""
config.py (not_interesting)
---------------------------
Configuration for the 'not_interesting' subscriber service.
"""

import os
from typing import List

KAFKA_BOOTSTRAP: List[str] = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")
GROUP_ID: str = os.getenv("GROUP_ID", "not-interesting-subscriber-group")

MONGO_URI: str = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB: str = os.getenv("MONGO_DB", "newsdb")
COLLECTION_NAME: str = os.getenv("MONGO_COLL", "not_interesting_messages")

AUTO_OFFSET_RESET: str = os.getenv("AUTO_OFFSET_RESET", "earliest")
MAX_POLL_INTERVAL_MS: int = int(os.getenv("MAX_POLL_INTERVAL_MS", "300000"))
SESSION_TIMEOUT_MS: int = int(os.getenv("SESSION_TIMEOUT_MS", "10000"))
