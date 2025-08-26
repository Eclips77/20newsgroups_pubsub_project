import json
from typing import List
from kafka import KafkaProducer
from . import config


class ProducerService:
    """
    OOP wrapper around KafkaProducer with a minimal surface.
    """

    def __init__(
        self,
        bootstrap_servers: list[str] | None = None,
        linger_ms: int = 10,
        acks: str = "all",
    ) -> None:
        if bootstrap_servers is None:
            bootstrap_servers = config.KAFKA_BOOTSTRAP
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=linger_ms,
            acks=acks,
        )

    @property
    def producer(self) -> KafkaProducer:
        return self._producer

    def publish_group(self, messages: List[dict], topic: str) -> int:
        """
        Publish a list of messages to a kafka topic and flush.

        Returns the number of messages sent.
        """
        count = 0
        for m in messages:
            self._producer.send(topic, value=m)
            count += 1
        self._producer.flush()
        return count

