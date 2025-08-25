import os
from typing import List

KAFKA_BOOTSTRAP: List[str] = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092").split(",")
KAFKA_TOPIC_INTERESTING: str = os.getenv("KAFKA_TOPIC_INTERESTING", "interesting")
KAFKA_TOPIC_NOT_INTERESTING: str = os.getenv("KAFKA_TOPIC_NOT_INTERESTING", "not_interesting")

# Dataset subset can be: "train", "test", or "all"
DATASET_SUBSET: str = os.getenv("DATASET_SUBSET", "train")
