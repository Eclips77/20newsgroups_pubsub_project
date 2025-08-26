import json
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
from kafka.errors import KafkaError
from pymongo import MongoClient, ASCENDING, errors
from . import config
import logging

logger = logging.getLogger(__name__)


class InterestingConsumerService:
    """
    OOP consumer service for the 'interesting' topic.
    Follows Single Responsibility Principle - only responsible for consuming and storing messages.
    """

    def __init__(self) -> None:
        """
        Initialize the consumer service with proper error handling.
        
        Raises:
            Exception: If initialization fails
        """
        logger.info("Initializing InterestingConsumerService")
        
        try:
            self._consumer = self._create_consumer()
            self._client, self._coll = self._setup_mongodb()
            logger.info("InterestingConsumerService initialized successfully")
            
        except Exception as e:
            logger.error("Failed to initialize InterestingConsumerService: %s", str(e), exc_info=True)
            raise

    def _create_consumer(self) -> KafkaConsumer:
        """
        Create and configure Kafka consumer.
        
        Returns:
            KafkaConsumer: Configured consumer instance
            
        Raises:
            Exception: If consumer creation fails
        """
        try:
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC,
                bootstrap_servers=config.KAFKA_BOOTSTRAP,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                enable_auto_commit=False,
                auto_offset_reset=config.AUTO_OFFSET_RESET,
                group_id=config.GROUP_ID,
                max_poll_interval_ms=config.MAX_POLL_INTERVAL_MS,
                session_timeout_ms=config.SESSION_TIMEOUT_MS,
            )
            logger.info("Kafka consumer created for topic: %s", config.KAFKA_TOPIC)
            return consumer
            
        except Exception as e:
            logger.error("Failed to create Kafka consumer: %s", str(e), exc_info=True)
            raise

    def _setup_mongodb(self) -> tuple[MongoClient, Any]:
        """
        Setup MongoDB connection and collection.
        
        Returns:
            tuple: MongoDB client and collection
            
        Raises:
            Exception: If MongoDB setup fails
        """
        try:
            client = MongoClient(
                config.MONGO_URI, 
                tz_aware=True, 
                uuidRepresentation="standard"
            )
            
            # Test connection
            client.admin.command('ping')
            logger.info("MongoDB connection established to: %s", config.MONGO_URI)
            
            collection = client[config.MONGO_DB][config.COLLECTION_NAME]
            
            # Create indexes with error handling
            try:
                collection.create_index([("kafka_offset", ASCENDING)], unique=True)
                collection.create_index([("timestamp", ASCENDING)])
                logger.info("MongoDB indexes created successfully")
            except errors.OperationFailure as e:
                logger.warning("Index creation failed (may already exist): %s", str(e))
            
            return client, collection
            
        except Exception as e:
            logger.error("Failed to setup MongoDB: %s", str(e), exc_info=True)
            raise

    @property
    def consumer(self) -> KafkaConsumer:
        """Get the Kafka consumer instance."""
        return self._consumer

    @property
    def collection(self):
        """Get the MongoDB collection instance."""
        return self._coll
    
    def consume_once(self, max_records: int = 10, poll_timeout_ms: int = 200) -> Dict[str, Any]:
        """
        Consume up to `max_records` and return consumption summary.
        Designed to be called from an HTTP endpoint without blocking.
        
        Args:
            max_records: Maximum number of records to consume
            poll_timeout_ms: Timeout for polling in milliseconds
            
        Returns:
            Dict containing consumption summary
            
        Raises:
            Exception: If consumption fails
        """
        logger.info("Starting message consumption (max_records=%d, timeout=%dms)", 
                   max_records, poll_timeout_ms)
        
        consumed = 0
        stored = 0
        errors_count = 0
        
        try:
            records = self._consumer.poll(timeout_ms=poll_timeout_ms, max_records=max_records)
            
            if not records:
                logger.debug("No messages received in this poll")
                return {"consumed": 0, "stored": 0, "errors": 0}
            
            for topic_partition, msgs in records.items():
                logger.debug("Processing %d messages from partition %s", 
                           len(msgs), topic_partition.partition)
                
                for msg in msgs:
                    consumed += 1
                    try:
                        success = self._process_message(msg)
                        if success:
                            stored += 1
                        else:
                            errors_count += 1
                            
                    except Exception as e:
                        logger.error("Error processing individual message: %s", str(e))
                        errors_count += 1
                        continue
            
            # Commit only if we processed messages successfully
            if stored > 0:
                self._consumer.commit()
                logger.info("Successfully processed %d/%d messages, committed offset", 
                          stored, consumed)
            
            return {
                "consumed": consumed,
                "stored": stored, 
                "errors": errors_count
            }
            
        except KafkaError as e:
            logger.error("Kafka error during consumption: %s", str(e), exc_info=True)
            raise
        except Exception as e:
            logger.error("Unexpected error during consumption: %s", str(e), exc_info=True)
            raise

    def _process_message(self, msg) -> bool:
        """
        Process a single message and store it in MongoDB.
        
        Args:
            msg: Kafka message
            
        Returns:
            bool: True if processing succeeded, False otherwise
        """
        try:
            doc: Dict[str, Any] = {
                "kafka_offset": msg.offset,
                "topic": msg.topic,
                "partition": msg.partition,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "value": msg.value,
                "key": msg.key.decode('utf-8') if msg.key else None,
            }
            
            self._coll.insert_one(doc)
            logger.debug("Stored message with offset %d from partition %d", 
                        msg.offset, msg.partition)
            return True
            
        except errors.DuplicateKeyError:
            logger.debug("Duplicate message ignored (offset: %d)", msg.offset)
            return True  # Consider this successful - message already exists
            
        except Exception as e:
            logger.error("Error storing message (offset: %d): %s", msg.offset, str(e))
            return False

    def close(self) -> None:
        """
        Close consumer and MongoDB connections.
        """
        try:
            if hasattr(self, '_consumer') and self._consumer:
                self._consumer.close()
                logger.info("Kafka consumer closed")
                
            if hasattr(self, '_client') and self._client:
                self._client.close()
                logger.info("MongoDB client closed")
                
        except Exception as e:
            logger.error("Error closing connections: %s", str(e), exc_info=True)


