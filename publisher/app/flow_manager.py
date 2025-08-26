from .producer import ProducerService
from .data_analyzer import DataAnalyzer
from . import config
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class FlowManager:
    """
    Orchestrates the flow of fetching data and publishing to Kafka topics.
    Follows Single Responsibility Principle - only responsible for coordinating the flow.
    """
    
    def __init__(self) -> None:
        try:
            self._producer = ProducerService()
            self._data_analyzer = DataAnalyzer(subset=config.DATASET_SUBSET)
            logger.info("FlowManager initialized successfully")
        except Exception as e:
            logger.error("Failed to initialize FlowManager: %s", str(e), exc_info=True)
            raise
    

    def fetch_and_publish(self) -> Dict[str, Any]:
        """
        Fetches a sample of messages, categorizes them as 'interesting' or 'not interesting',
        and publishes each group to their respective Kafka topics.
        
        Returns:
            Dict[str, Any]: A dictionary containing the results of the publish operations for both
            'interesting' and 'not interesting' message groups.
            
        Raises:
            Exception: If data fetching or publishing fails
        """
        logger.info("Starting fetch and publish process")
        
        try:
            # Fetch data
            data = self._data_analyzer.sample_messages()
            if not data or not data.get("interesting") or not data.get("not_interesting"):
                raise ValueError("No data fetched from analyzer")
            
            logger.info("Successfully fetched %d interesting and %d not_interesting messages", 
                       len(data["interesting"]), len(data["not_interesting"]))
            
            # Publish interesting messages
            sent_interesting = self._producer.publish_group(
                data["interesting"], config.KAFKA_TOPIC_INTERESTING
            )
            
            # Publish not interesting messages
            sent_not_interesting = self._producer.publish_group(
                data["not_interesting"], config.KAFKA_TOPIC_NOT_INTERESTING
            )
            
            result = {
                "published": {
                    "interesting": sent_interesting, 
                    "not_interesting": sent_not_interesting
                }
            }
            
            logger.info("Successfully completed fetch and publish process: %s", result)
            return result
            
        except Exception as e:
            logger.error("Error in fetch and publish process: %s", str(e), exc_info=True)
            raise
