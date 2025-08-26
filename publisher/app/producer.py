import json
from typing import List
from kafka import KafkaProducer
from kafka.errors import KafkaError
from . import config
import logging

logger = logging.getLogger(__name__)


class ProducerService:
    """
    OOP wrapper around KafkaProducer with a minimal surface.
    Follows Single Responsibility Principle - only responsible for publishing messages to Kafka.
    """

    def __init__(
        self,
        bootstrap_servers: list[str] | None = None,
        linger_ms: int = 10,
        acks: str = "all",
    ) -> None:
        """
        Initialize KafkaProducer with proper error handling.
        
        Args:
            bootstrap_servers: List of Kafka bootstrap servers
            linger_ms: Time to wait for additional messages before sending
            acks: Number of acknowledgments the producer requires
            
        Raises:
            Exception: If KafkaProducer initialization fails
        """
        if bootstrap_servers is None:
            bootstrap_servers = config.KAFKA_BOOTSTRAP
            
        logger.info("Initializing KafkaProducer with servers: %s", bootstrap_servers)
        
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=linger_ms,
                acks=acks,
            )
            logger.info("KafkaProducer initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize KafkaProducer: %s", str(e), exc_info=True)
            raise

    @property
    def producer(self) -> KafkaProducer:
        """Get the underlying KafkaProducer instance."""
        return self._producer

    def publish_group(self, messages: List[dict], topic: str) -> int:
        """
        Publish a list of messages to a kafka topic and flush.

        Args:
            messages: List of message dictionaries to publish
            topic: Kafka topic name
            
        Returns:
            int: Number of messages sent
            
        Raises:
            ValueError: If messages is empty or topic is invalid
            KafkaError: If publishing fails
        """
        if not messages:
            logger.warning("No messages to publish to topic '%s'", topic)
            return 0
            
        if not topic or not isinstance(topic, str):
            raise ValueError(f"Invalid topic: {topic}")
            
        logger.info("Starting to publish %d messages to topic '%s'", len(messages), topic)
        
        count = 0
        failed_messages = []
        
        try:
            for i, message in enumerate(messages):
                if not isinstance(message, dict):
                    logger.warning("Skipping invalid message at index %d: %s", i, type(message))
                    continue
                    
                try:
                    future = self._producer.send(topic, value=message)
                    # Optional: wait for confirmation for critical operations
                    # record_metadata = future.get(timeout=10)
                    count += 1
                    logger.debug("Queued message %d for topic '%s'", count, topic)
                except Exception as e:
                    failed_messages.append((i, str(e)))
                    logger.warning("Failed to queue message %d: %s", i, str(e))
            
            # Flush to ensure all messages are sent
            self._producer.flush()
            
            if failed_messages:
                logger.warning("Failed to publish %d messages: %s", len(failed_messages), failed_messages)
            
            logger.info("Successfully published %d messages to topic '%s'", count, topic)
            return count
            
        except KafkaError as e:
            logger.error("Kafka error while publishing to topic '%s': %s", topic, str(e), exc_info=True)
            raise
        except Exception as e:
            logger.error("Unexpected error while publishing to topic '%s': %s", topic, str(e), exc_info=True)
            raise

    def close(self) -> None:
        """
        Close the producer and clean up resources.
        """
        try:
            if hasattr(self, '_producer') and self._producer:
                self._producer.close()
                logger.info("KafkaProducer closed successfully")
        except Exception as e:
            logger.error("Error closing KafkaProducer: %s", str(e), exc_info=True)

