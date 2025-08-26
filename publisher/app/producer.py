import json
from typing import List
from kafka import KafkaProducer
from . import config


def get_producer() -> KafkaProducer:
    """
    Create and return a configured KafkaProducer.

    Returns:
        KafkaProducer configured with JSON value serializer.
    """
    return KafkaProducer(
        bootstrap_servers=config.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=10,
        acks="all",
    )

def publish_group(messages: List[dict], topic: str) -> int:
    """
    Publish a list of messages to a kafka topic.

    Args:
        messages: List of message dicts to publish.
        topic: Kafka topic name.

    Returns:
        Number of messages successfully sent (after flush).
    """
    producer = get_producer()
    count = 0
    for m in messages:
        producer.send(topic, value=m)
        count += 1
    producer.flush()
    return count
